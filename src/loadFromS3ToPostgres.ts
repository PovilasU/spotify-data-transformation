import AWS from "aws-sdk";
import { Client } from "pg";
import path from "path";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync"; // Import parse from the sync version of csv-parse
import cliProgress from "cli-progress";
import { createLogger, format, transports } from "winston";

// Load environment variables from .env file (assumed one level up)
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

// Configure Winston logger
const logger = createLogger({
  level: "info",
  format: format.combine(
    format.timestamp(),
    format.printf(
      ({ timestamp, level, message }) => `${timestamp} [${level}] ${message}`
    )
  ),
  transports: [
    new transports.Console(),
    new transports.File({
      filename: path.join(__dirname, "..", "logs", "app.log"),
    }),
  ],
});

// Set up AWS S3 client
const s3 = new AWS.S3({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID, // AWS credentials
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION, // Example: 'us-east-1'
});

// S3 bucket and file key
const bucketName = process.env.S3_BUCKET_NAME || "";
const fileKey = "transformedTracks.csv"; // The file to be downloaded from S3

/**
 * Ensure the target PostgreSQL database exists.
 * If it doesn't exist, this function creates it.
 */
async function ensureDatabaseExists(targetDb: string): Promise<void> {
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
 */
async function downloadCSVFromS3(): Promise<string> {
  try {
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: fileKey,
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
 * Main function that downloads the CSV from S3 and loads it into PostgreSQL.
 */
async function loadFromS3ToPostgres(): Promise<void> {
  try {
    const targetDb = process.env.PG_DATABASE;
    if (!targetDb) {
      logger.error("Environment variable PG_DATABASE is not set.");
      process.exit(1);
    }

    // Ensure the target database exists
    await ensureDatabaseExists(targetDb);

    // Connect to the target database
    const pgClient = new Client({
      host: process.env.PG_HOST,
      port: parseInt(process.env.PG_PORT || "5432"),
      user: process.env.PG_USER,
      password: process.env.PG_PASSWORD,
      database: targetDb,
    });
    await pgClient.connect();
    logger.info(`Connected to PostgreSQL database "${targetDb}"`);

    // Download CSV file from S3
    const fileContent = await downloadCSVFromS3();

    // Parse CSV file (using headers as keys)
    const records = parse(fileContent, {
      columns: true,
      skip_empty_lines: true,
    });
    if (records.length === 0) {
      logger.error("CSV file is empty.");
      return;
    }

    // Use CSV headers from the first record
    const headers = Object.keys(records[0]);

    // Create table with columns as TEXT (if it doesn't exist)
    const columnsDefinition = headers.map((col) => `"${col}" TEXT`).join(", ");
    const createTableQuery = `CREATE TABLE IF NOT EXISTS tracks (${columnsDefinition});`;
    await pgClient.query(createTableQuery);
    logger.info(
      `Ensured table 'tracks' exists with columns: ${headers.join(", ")}`
    );

    // Prepare the INSERT query (using dynamic placeholders)
    const columnsList = headers.map((col) => `"${col}"`).join(", ");
    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
    const insertQuery = `INSERT INTO tracks (${columnsList}) VALUES (${placeholders})`;

    // Set up the progress bar
    const progressBar = new cliProgress.SingleBar(
      { format: "Loading [{bar}] {value}/{total} rows", hideCursor: true },
      cliProgress.Presets.shades_classic
    );
    progressBar.start(records.length, 0);

    // Insert each record one by one
    for (const record of records) {
      const values = headers.map((header) => record[header] ?? null);
      await pgClient.query(insertQuery, values);
      progressBar.increment();
    }
    progressBar.stop();
    logger.info(`Loaded ${records.length} rows into PostgreSQL.`);
    await pgClient.end();
    logger.info("Disconnected from PostgreSQL");
  } catch (err) {
    logger.error("Error during CSV load: " + err);
  }
}

loadFromS3ToPostgres().catch((err) => {
  logger.error("Unhandled error: " + err);
});
