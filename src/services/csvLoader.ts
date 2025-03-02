import { Client } from "pg";
import { parse } from "csv-parse/sync"; // Import parse from the sync version of csv-parse
import cliProgress from "cli-progress";
import { downloadCSVFromS3 } from "./s3Service";
import { ensureDatabaseExists } from "./dbService";
import { config } from "../config/config";
import { logger } from "../utils/logger";

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
 * Converts and validates a CSV value based on the expected column type.
 * If the CSV field is an empty string, it will return null for non-TEXT types.
 * Also, for array columns, it converts a JS-style string (e.g. "['a','b']")
 * into a PostgreSQL array literal (e.g. "{a,b}").
 */
function convertValue(value: string, column: string): any {
  const type = getColumnType(column);
  // For array columns, convert the value format.
  if (value && (column === "id_artists" || column === "genres")) {
    // Convert a string like "['45tIt06XoI0Iio4LBEVpls']" to "{45tIt06XoI0Iio4LBEVpls}"
    value = value.replace(/^\s*\[\s*/, "{").replace(/\s*\]\s*$/, "}");
    value = value.replace(/'/g, "");
    return value;
  }
  // For non-text types, empty strings should be treated as null.
  if (value === "") {
    return null;
  }
  // Convert based on the expected type.
  if (type === "INTEGER") {
    const intVal = parseInt(value, 10);
    return isNaN(intVal) ? null : intVal;
  }
  if (type === "DOUBLE PRECISION") {
    const floatVal = parseFloat(value);
    return isNaN(floatVal) ? null : floatVal;
  }
  if (type === "BOOLEAN") {
    const lowerVal = value.toLowerCase();
    if (lowerVal === "true" || value === "1") return true;
    if (lowerVal === "false" || value === "0") return false;
    return null;
  }
  // For TEXT (and any unhandled types), return the value as is.
  return value;
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
    const targetDb = config.postgres.database;
    if (!targetDb) {
      logger.error("Environment variable PG_DATABASE is not set.");
      process.exit(1);
    }

    // Ensure the target database exists.
    await ensureDatabaseExists(targetDb);

    // Connect to the target database.
    const pgClient = new Client({
      host: config.postgres.host,
      port: config.postgres.port,
      user: config.postgres.user,
      password: config.postgres.password,
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

    // Validate required headers.
    const requiredHeaders = ["id", "name"];
    const missingHeaders = requiredHeaders.filter((h) => !headers.includes(h));
    if (missingHeaders.length > 0) {
      throw new Error(
        `Missing required CSV headers for table "${tableName}": ${missingHeaders.join(
          ", "
        )}`
      );
    }

    // Create table with columns using dynamic types via getColumnType.
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
      const values = headers.map((header) => {
        const rawValue = record[header] ?? "";
        return convertValue(rawValue, header);
      });
      await pgClient.query(insertQuery, values);
      progressBar.increment();
    }
    progressBar.stop();
    logger.info(`Loaded ${records.length} rows into table "${tableName}".`);
    await pgClient.end();
    logger.info("Disconnected from PostgreSQL");
  } catch (err) {
    logger.error(`Error during CSV load into table "${tableName}": ${err}`);
    throw err; // rethrow error so that the promise rejects
  }
}
