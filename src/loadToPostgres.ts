import { Client } from "pg";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { parse } from "csv-parse/sync";
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

// Path to CSV file (assumed to be in a folder called "data" one level up)
const filePath = path.join(__dirname, "..", "data", "transformedTracks.csv");

// Ensure the target database exists
async function ensureDatabaseExists(targetDb: string) {
  const adminClient = new Client({
    host: process.env.PG_HOST,
    port: parseInt(process.env.PG_PORT || "5432"),
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
    database: "postgres", // Connect to default database for admin tasks
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
      // Note: CREATE DATABASE cannot be parameterized.
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

async function loadCSVToPostgres() {
  try {
    // Get target database from environment variables.
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

    // Read and parse the CSV file into memory.
    const fileContent = fs.readFileSync(filePath, "utf8");
    const records = parse(fileContent, {
      columns: true, // Use first line as header names.
      skip_empty_lines: true,
    });
    if (records.length === 0) {
      throw new Error("CSV file is empty.");
    }

    // Use CSV header keys from the first record.
    const headers = Object.keys(records[0]);

    // Create table with columns as TEXT.
    const columnsDefinition = headers.map((col) => `"${col}" TEXT`).join(", ");
    const createTableQuery = `CREATE TABLE IF NOT EXISTS tracks (${columnsDefinition});`;
    await pgClient.query(createTableQuery);
    logger.info(
      `Ensured table 'tracks' exists with columns: ${headers.join(", ")}`
    );

    // Prepare the INSERT query.
    const columnsList = headers.map((col) => `"${col}"`).join(", ");
    const placeholders = headers.map((_, i) => `$${i + 1}`).join(", ");
    const insertQuery = `INSERT INTO tracks (${columnsList}) VALUES (${placeholders})`;

    // Set up the progress bar.
    const progressBar = new cliProgress.SingleBar(
      {
        format: "Loading [{bar}] {value}/{total} rows",
        hideCursor: true,
      },
      cliProgress.Presets.shades_classic
    );
    progressBar.start(records.length, 0);

    // Insert each record one by one.
    for (const record of records) {
      const values = headers.map((header) =>
        record[header] !== undefined ? record[header] : null
      );
      await pgClient.query(insertQuery, values);
      progressBar.increment();
    }
    progressBar.stop();
    logger.info(`Loaded ${records.length} rows into PostgreSQL`);

    await pgClient.end();
    logger.info("Disconnected from PostgreSQL");
  } catch (err) {
    logger.error("Error during CSV load: " + err);
  }
}

loadCSVToPostgres().catch((err) => {
  logger.error("Unhandled error: " + err);
});
