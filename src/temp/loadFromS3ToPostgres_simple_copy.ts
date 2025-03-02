//loadFromS3ToPostgresNew.ts
import AWS from "aws-sdk";
import { Client } from "pg";
import path from "path";
import fs from "fs";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync"; // Import parse from the sync version of csv-parse
import cliProgress from "cli-progress";
import { logger } from "./utils/logger";

// Load environment variables from .env file (assumed one level up)
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

// Set up AWS S3 client
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID, // AWS credentials
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION, // Example: 'us-east-1'
});

// S3 bucket and file keys
const bucketName = process.env.S3_BUCKET_NAME || "";
const tracksFileKey = "transformedTracks.csv";
const artistsFileKey = "transformedArtists.csv";

/**
 *
 * Ensure the target PostgreSQL database exists.
 * If it doesn't exist, this function creates it.
 */
export async function ensureDatabaseExists(targetDb: string): Promise<void> {
  const adminClient = new Client({
    host: process.env.PG_HOST,
    port: parseInt(process.env.PG_PORT || "5432"),
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    database: "postgres", // Connect to default db for admin tasks
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
      // CREATE DATABASE cannot be parameterized.
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
 * Download CSV file from S3 and return its content as a string.
 * @param s3Key - The S3 key of the file to download.
 */
export async function downloadCSVFromS3(s3Key: string): Promise<string> {
  try {
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: s3Key,
    };
    const data = await s3.getObject(params).promise();
    if (!data.Body) {
      throw new Error("No file content from S3");
    }
    return data.Body.toString(); // Convert Buffer to string
  } catch (err) {
    logger.error("Error downloading CSV from S3: " + err);
    throw err;
  }
}

/**
 * Load a CSV file (from S3) into a PostgreSQL table.
 * @param s3Key - The S3 key for the CSV file.
 * @param tableName - The target table name.
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

    // Ensure the target database exists.
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

    // Download CSV file from S3.
    const fileContent = await downloadCSVFromS3(s3Key);

    // Parse CSV file (using headers as keys)
    const records = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
    });
    if (records.length === 0) {
      logger.error(`CSV file for ${tableName} is empty.`);
      await pgClient.end();
      return;
    }

    // Use CSV headers from the first record.
    const headers = Object.keys(records[0]);

    // Create table with columns as TEXT (if it doesn't exist)
    const columnsDefinition = headers.map((col) => `"${col}" TEXT`).join(", ");
    const createTableQuery = `CREATE TABLE IF NOT EXISTS ${tableName} (${columnsDefinition});`;
    await pgClient.query(createTableQuery);
    logger.info(
      `Ensured table "${tableName}" exists with columns: ${headers.join(", ")}`
    );

    // Prepare the INSERT query (using dynamic placeholders)
    const columnsList = headers.map((col) => `"${col}"`).join(", ");
    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
    const insertQuery = `INSERT INTO ${tableName} (${columnsList}) VALUES (${placeholders})`;

    // Set up the progress bar.
    const progressBar = new cliProgress.SingleBar(
      {
        format: "Loading into " + tableName + " [{bar}] {value}/{total} rows",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic
    );
    progressBar.start(records.length, 0);

    // Insert each record one by one.
    for (const record of records) {
      const values = headers.map((header) => record[header] ?? null);
      await pgClient.query(insertQuery, values);
      progressBar.increment();
    }
    progressBar.stop();
    logger.info(`Loaded ${records.length} rows into table "${tableName}".`);
    await pgClient.end();
    logger.info("Disconnected from PostgreSQL");
  } catch (err) {
    logger.error(`Error during CSV load into table "${tableName}": ${err}`);
  }
}

// Run the pipelines to load both tracks and artists CSV files.
async function runLoads() {
  await loadCSVIntoTable(tracksFileKey, "tracks");
  await loadCSVIntoTable(artistsFileKey, "artists");
}

runLoads().catch((err) => {
  logger.error("Unhandled error: " + err);
});
