import { Client } from "pg";
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
 * Ensure the target PostgreSQL database exists.
 * If it doesn't exist, this function creates it.
 */
export async function ensureDatabaseExists(targetDb: string): Promise<void> {
  const adminClient = new Client({
    host: config.postgres.host,
    port: config.postgres.port,
    user: config.postgres.user,
    password: config.postgres.password,
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
