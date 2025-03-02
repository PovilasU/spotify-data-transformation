import AWS from "aws-sdk";
import { Client } from "pg";
import path from "path";
import fs from "fs";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync";
import cliProgress from "cli-progress";
import { logger } from "./utils/logger";

// --- Load environment variables and validate configuration ---
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });
const requiredVars = [
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_REGION",
  "S3_BUCKET_NAME",
  "PG_HOST",
  "PG_PORT",
  "PG_USER",
  "PG_PASSWORD",
  "PG_DATABASE",
];
const missing = requiredVars.filter((v) => !process.env[v]);
if (missing.length > 0) {
  throw new Error(`Missing required env variables: ${missing.join(", ")}`);
}

// --- AWS S3 Setup ---
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});
const bucketName = process.env.S3_BUCKET_NAME || "";
const tracksFileKey = "transformedTracks.csv";
const artistsFileKey = "transformedArtists.csv";

// --- Helper functions for type conversion and validation ---
export function safeConvert(
  value: any,
  type: "integer" | "double" | "boolean" | "text"
): any {
  if (value === undefined || value === null || value === "") return null;
  try {
    switch (type) {
      case "integer":
        const intVal = parseInt(parseFloat(value).toString(), 10);
        return isNaN(intVal) ? null : intVal;
      case "double":
        const dVal = parseFloat(value);
        return isNaN(dVal) ? null : dVal;
      case "boolean":
        return value.toLowerCase() === "true" || value === "1";
      case "text":
      default:
        return value.toString().trim();
    }
  } catch (err) {
    logger.error(
      `Conversion error for value "${value}" to type ${type}: ${err}`
    );
    return null;
  }
}

/**
 * Returns a readable stream from a local file or S3.
 */
async function downloadCSV(s3Key: string): Promise<string> {
  try {
    // For simplicity, we assume local files if LOCAL_TEST is true.
    if (process.env.LOCAL_TEST === "true") {
      let filePath: string;
      if (s3Key === tracksFileKey) {
        filePath = path.join(__dirname, "..", "data", "transformedTracks.csv");
      } else if (s3Key === artistsFileKey) {
        filePath = path.join(__dirname, "..", "data", "transformedArtists.csv");
      } else {
        throw new Error(`Local file for key "${s3Key}" is not configured.`);
      }
      logger.info(`Reading CSV from local file: ${filePath}`);
      return await fs.promises.readFile(filePath, "utf-8");
    } else {
      logger.info(`Downloading CSV from S3 key: ${s3Key}`);
      const params: AWS.S3.Types.GetObjectRequest = {
        Bucket: bucketName,
        Key: s3Key,
      };
      const data = await s3.getObject(params).promise();
      if (!data.Body) {
        throw new Error("No file content from S3");
      }
      return data.Body.toString();
    }
  } catch (err) {
    logger.error(`Error downloading CSV for key "${s3Key}": ${err}`);
    throw err;
  }
}

/**
 * Maps CSV header names to PostgreSQL data types.
 */
function getColumnType(column: string): string {
  switch (column) {
    case "id":
      return "TEXT";
    case "name":
      return "TEXT";
    case "popularity":
      return "INTEGER";
    case "duration_ms":
      return "INTEGER";
    case "explicit":
      return "BOOLEAN";
    case "artists":
      return "TEXT"; // comma-separated string
    case "id_artists":
      return "TEXT[]"; // array literal
    case "release_date":
      return "TEXT";
    case "release_year":
      return "INTEGER";
    case "release_month":
      return "INTEGER";
    case "release_day":
      return "INTEGER";
    case "danceability":
      return "DOUBLE PRECISION";
    case "energy":
      return "DOUBLE PRECISION";
    case "key":
      return "INTEGER";
    case "loudness":
      return "DOUBLE PRECISION";
    case "mode":
      return "INTEGER";
    case "speechiness":
      return "DOUBLE PRECISION";
    case "acousticness":
      return "DOUBLE PRECISION";
    case "instrumentalness":
      return "DOUBLE PRECISION";
    case "liveness":
      return "DOUBLE PRECISION";
    case "valence":
      return "DOUBLE PRECISION";
    case "tempo":
      return "DOUBLE PRECISION";
    case "time_signature":
      return "INTEGER";
    case "danceability_level":
      return "TEXT";
    case "followers":
      return "INTEGER";
    case "genres":
      return "TEXT[]";
    default:
      return "TEXT";
  }
}

/**
 * Ensures that the target PostgreSQL database exists.
 */
export async function ensureDatabaseExists(targetDb: string): Promise<void> {
  const adminClient = new Client({
    host: process.env.PG_HOST,
    port: parseInt(process.env.PG_PORT || "5432"),
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    database: "postgres",
  });
  try {
    await adminClient.connect();
    const res = await adminClient.query(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [targetDb]
    );
    if (res.rowCount === 0) {
      logger.info(
        `Database "${targetDb}" does not exist. Creating database...`
      );
      await adminClient.query(`CREATE DATABASE "${targetDb}"`);
      logger.info(`Database "${targetDb}" created successfully.`);
    } else {
      logger.info(`Database "${targetDb}" already exists.`);
    }
  } catch (err) {
    logger.error("Error ensuring database exists: " + err);
    process.exit(1);
  } finally {
    await adminClient.end();
  }
}

/**
 * Loads the CSV data into the target PostgreSQL table using Node (batch inserts).
 */
export async function loadCSVIntoTable(
  s3Key: string,
  tableName: string
): Promise<void> {
  try {
    const targetDb = process.env.PG_DATABASE;
    if (!targetDb) {
      logger.error("Environment variable PG_DATABASE is not set.");
      process.exit(1);
    }

    // Ensure database exists.
    await ensureDatabaseExists(targetDb);

    // Connect to the target database.
    const pgClient = new Client({
      host: process.env.PG_HOST,
      port: parseInt(process.env.PG_PORT || "5432"),
      user: process.env.PG_USER,
      password: process.env.PG_PASSWORD,
      database: targetDb,
    });
    await pgClient.connect();
    logger.info(`Connected to PostgreSQL database "${targetDb}"`);

    // Download CSV content.
    const fileContent = await downloadCSV(s3Key);

    // Parse CSV.
    let records: any[];
    try {
      records = parse(fileContent, {
        columns: true,
        skip_empty_lines: true,
      });
    } catch (parseErr) {
      logger.error(
        `Error parsing CSV file for table "${tableName}": ${parseErr}`
      );
      throw new Error(`Error parsing CSV file for table "${tableName}"`);
    }

    if (records.length === 0) {
      logger.error(`CSV file for ${tableName} is empty.`);
      await pgClient.end();
      return;
    }

    // Validate required headers.
    const headers = Object.keys(records[0]);
    const requiredHeaders = ["id", "name"]; // Add more required headers as needed.
    const missingHeaders = requiredHeaders.filter((h) => !headers.includes(h));
    if (missingHeaders.length > 0) {
      throw new Error(
        `Missing required CSV headers for table "${tableName}": ${missingHeaders.join(
          ", "
        )}`
      );
    }

    // Create table with all columns as TEXT (for simplicity) or use getColumnType for dynamic types.
    const columnsDefinition = headers
      .map((col) => `"${col}" ${getColumnType(col)}`)
      .join(", ");
    const createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (${columnsDefinition});`;
    await pgClient.query(createTableQuery);
    logger.info(
      `Ensured table "${tableName}" exists with columns: ${headers.join(", ")}`
    );

    // Prepare the INSERT query using dynamic placeholders.
    const columnsList = headers.map((col) => `"${col}"`).join(", ");
    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
    const insertQuery = `INSERT INTO ${tableName} (${columnsList}) VALUES (${placeholders})`;

    // Process each record for transformation and sanitization.
    let processedCount = 0;
    let skippedCount = 0;
    for (const record of records) {
      try {
        // Sanitize each field.
        headers.forEach((header) => {
          if (typeof record[header] === "string") {
            record[header] = record[header].trim();
          }
        });
        // Transform fields.
        if (record["artists"]) {
          try {
            const regex = /(['"])(.*?)\1/g;
            const matches = record["artists"].match(regex);
            if (matches) {
              const arr = matches.map((s: string) => s.slice(1, -1));
              record["artists"] = arr.join(",");
            }
          } catch (e) {
            logger.error(
              `Error parsing 'artists' in record ${processedCount}: ${record["artists"]}`
            );
            throw e;
          }
        }
        if (record["id_artists"]) {
          try {
            const normalized = record["id_artists"].replace(/'/g, '"');
            const arr = JSON.parse(normalized) as string[];
            record["id_artists"] = "{" + arr.join(",") + "}";
          } catch (e) {
            logger.error(
              `Error parsing 'id_artists' in record ${processedCount}: ${record["id_artists"]}`
            );
            throw e;
          }
        }
        if (record["genres"]) {
          if (record["genres"] === "[]" || record["genres"] === "") {
            record["genres"] = "{}";
          } else {
            try {
              const regex = /(['"])(.*?)\1/g;
              const matches = record["genres"].match(regex);
              if (matches) {
                const arr = matches.map((s: string) => s.slice(1, -1));
                record["genres"] = "{" + arr.join(",") + "}";
              } else {
                record["genres"] = "{}";
              }
            } catch (e) {
              logger.error(
                `Error parsing 'genres' in record ${processedCount}: ${record["genres"]}`
              );
              throw e;
            }
          }
        }
        if (tableName === "artists") {
          if (record["followers"] != null) {
            record["followers"] = safeConvert(record["followers"], "integer");
          }
          if (record["popularity"] != null) {
            record["popularity"] = safeConvert(record["popularity"], "integer");
          }
        }
        // Convert empty strings to null.
        headers.forEach((header) => {
          if (record[header] === "") {
            record[header] = null;
          }
        });

        const values = headers.map((header) => record[header]);
        await pgClient.query(insertQuery, values);
        processedCount++;
      } catch (err) {
        skippedCount++;
        logger.error(
          `Skipping record ${
            processedCount + skippedCount
          } due to error: ${err}. Record: ${JSON.stringify(record)}`
        );
      }
      if ((processedCount + skippedCount) % 10000 === 0) {
        logger.info(
          `Processed ${
            processedCount + skippedCount
          } records so far. Inserted: ${processedCount}, Skipped: ${skippedCount}`
        );
      }
    }
    logger.info(
      `Batch load complete for table "${tableName}". Total records: ${records.length}, Inserted: ${processedCount}, Skipped: ${skippedCount}`
    );
    await pgClient.end();
    logger.info("Disconnected from PostgreSQL");
  } catch (err) {
    logger.error(`Error during CSV load into table "${tableName}": ${err}`);
    throw err;
  }
}

/**
 * Main function to load CSV data for both tracks and artists.
 */
async function runLoads(): Promise<void> {
  await loadCSVIntoTable(tracksFileKey, "tracks");
  await loadCSVIntoTable(artistsFileKey, "artists");
}

runLoads().catch((err) => {
  logger.error("Unhandled error: " + err);
});
