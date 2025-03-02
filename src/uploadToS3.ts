import AWS from "aws-sdk";
import path from "path";
import fs from "fs";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync";
import cliProgress from "cli-progress";
import { logger } from "./logger";
import { ensureDatabaseExists, getClient } from "./db";
import { config } from "./config";

// Load environment variables (already loaded in config.ts, but in case you run this file standalone)
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

const s3 = new AWS.S3({
  accessKeyId: config.aws.accessKeyId,
  secretAccessKey: config.aws.secretAccessKey,
  region: config.aws.region,
});

const bucketName = config.aws.bucketName;
const tracksFileKey = "transformedTracks.csv";
const artistsFileKey = "transformedArtists.csv";

/**
 * Returns a readable stream from a local file (if LOCAL_TEST is enabled) or from S3.
 * Uses a highWaterMark of 128 KB.
 */
function getCSVReadStream(s3Key: string): NodeJS.ReadableStream {
  if (config.localTest) {
    let filePath: string;
    if (s3Key === tracksFileKey) {
      filePath = path.join(__dirname, "..", "data", "transformedTracks.csv");
    } else if (s3Key === artistsFileKey) {
      filePath = path.join(__dirname, "..", "data", "transformedArtists.csv");
    } else {
      throw new Error(`Local file for key "${s3Key}" is not configured.`);
    }
    logger.info(`Creating read stream from local file: ${filePath}`);
    return fs.createReadStream(filePath, { highWaterMark: 128 * 1024 });
  } else {
    logger.info(`Creating S3 read stream for key: ${s3Key}`);
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: s3Key,
    };
    return s3.getObject(params).createReadStream();
  }
}

/**
 * Safely converts a value to a specified type.
 */
function safeConvert(
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
      return "TEXT"; // Comma-separated string
    case "id_artists":
      return "TEXT[]"; // Array literal
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
 * Downloads CSV file from S3 and returns its content as a string.
 */
async function downloadCSVFromS3(s3Key: string): Promise<string> {
  try {
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: s3Key,
    };
    const data = await s3.getObject(params).promise();
    if (!data.Body) {
      throw new Error("No file content from S3");
    }
    return data.Body.toString();
  } catch (err) {
    logger.error("Error downloading CSV from S3: " + err);
    throw err;
  }
}

/**
 * Loads a CSV file into a PostgreSQL table.
 * Performs per-record asynchronous inserts with a concurrency limit and retry on failure.
 */
export async function loadCSVIntoTable(
  s3Key: string,
  tableName: string
): Promise<void> {
  try {
    const targetDb = config.pg.database;
    if (!targetDb) {
      logger.error("Environment variable PG_DATABASE is not set.");
      process.exit(1);
    }

    // Ensure the target database exists.
    await ensureDatabaseExists(targetDb);

    // Get CSV content.
    const fileContent = await downloadCSVFromS3(s3Key);

    // Parse CSV content.
    let records: any[];
    try {
      records = parse(fileContent, { columns: true, skip_empty_lines: true });
    } catch (parseErr) {
      logger.error(`Error parsing CSV for table "${tableName}": ${parseErr}`);
      throw parseErr;
    }
    if (records.length === 0) {
      logger.error(`CSV file for table "${tableName}" is empty.`);
      return;
    }

    // Extract headers.
    const headers = Object.keys(records[0]);
    const requiredHeaders = ["id", "name"];
    const missingHeaders = requiredHeaders.filter((h) => !headers.includes(h));
    if (missingHeaders.length > 0) {
      throw new Error(
        `Missing required CSV headers for table "${tableName}": ${missingHeaders.join(
          ", "
        )}`
      );
    }

    // Create table dynamically.
    const columnsDefinition = headers
      .map((col) => `"${col}" ${getColumnType(col)}`)
      .join(", ");
    const createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (${columnsDefinition});`;
    const client = await getClient();
    await client.query(createTableQuery);
    logger.info(
      `Ensured table "${tableName}" exists with columns: ${headers.join(", ")}`
    );

    // Prepare the per-record INSERT query.
    const columnsList = headers.map((col) => `"${col}"`).join(", ");
    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
    const insertQuery = `INSERT INTO ${tableName} (${columnsList}) VALUES (${placeholders})`;

    // Set up a progress bar.
    const progressBar = new cliProgress.SingleBar(
      {
        format: "Loading into " + tableName + " [{bar}] {value}/{total} rows",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic
    );
    progressBar.start(records.length, 0);

    let processedCount = 0;
    let skippedCount = 0;

    // Limit concurrency.
    const concurrency = 50;
    let current = 0;
    async function worker() {
      while (current < records.length) {
        const index = current++;
        const record = records[index];
        try {
          // Sanitize and transform record.
          headers.forEach((header) => {
            if (typeof record[header] === "string") {
              record[header] = record[header].trim();
            }
          });
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
                `Error parsing 'artists' in record ${index}: ${record["artists"]}`
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
                `Error parsing 'id_artists' in record ${index}: ${record["id_artists"]}`
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
                  `Error parsing 'genres' in record ${index}: ${record["genres"]}`
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
              record["popularity"] = safeConvert(
                record["popularity"],
                "integer"
              );
            }
          }
          headers.forEach((header) => {
            if (record[header] === "") {
              record[header] = null;
            }
          });
          const values = headers.map((header) => record[header]);
          // Retry insertion up to 3 times.
          await (async function retryInsert(): Promise<void> {
            let attempts = 0;
            while (attempts < 3) {
              try {
                await client.query(insertQuery, values);
                return;
              } catch (e) {
                attempts++;
                logger.error(
                  `Insert retry ${attempts} for record ${index} failed: ${e}`
                );
                if (attempts >= 3) throw e;
              }
            }
          })();
          processedCount++;
        } catch (err) {
          skippedCount++;
          logger.error(
            `Skipping record ${index} due to error: ${err}. Record: ${JSON.stringify(
              record
            )}`
          );
        }
        progressBar.increment();
      }
    }

    await Promise.all(
      Array(concurrency)
        .fill(null)
        .map(() => worker())
    );
    progressBar.stop();
    logger.info(
      `Load complete for table "${tableName}". Total records: ${records.length}, Inserted: ${processedCount}, Skipped: ${skippedCount}`
    );
    await client.end();
    logger.info("Disconnected from PostgreSQL");
  } catch (err) {
    logger.error(`Error during CSV load into table "${tableName}": ${err}`);
  }
}

/**
 * Main function to load CSV data for both tracks and artists.
 */
async function runLoads(): Promise<void> {
  // Uncomment one of these as needed:
  // await loadCSVIntoTable(tracksFileKey, "tracks");
  await loadCSVIntoTable(artistsFileKey, "artists");
}

runLoads().catch((err) => {
  logger.error("Unhandled error: " + err);
});
