import { Client } from "pg";
import { config } from "../config/config";
import { logger } from "../utils/logger";

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
