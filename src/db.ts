import { Client } from "pg";
import { config } from "./config";
import { logger } from "./utils/logger";

/**
 * Ensures that the target PostgreSQL database exists.
 * If it does not, this function creates it.
 */
export async function ensureDatabaseExists(targetDb: string): Promise<void> {
  const client = new Client({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: "postgres", // Use default database for admin tasks
  });
  try {
    await client.connect();
    const res = await client.query(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [targetDb]
    );
    if (res.rowCount === 0) {
      logger.info(
        `Database "${targetDb}" does not exist. Creating database...`
      );
      await client.query(`CREATE DATABASE "${targetDb}"`);
      logger.info(`Database "${targetDb}" created successfully.`);
    } else {
      logger.info(`Database "${targetDb}" already exists.`);
    }
  } catch (err) {
    logger.error("Error ensuring database exists: " + err);
    process.exit(1);
  } finally {
    await client.end();
  }
}

/**
 * Returns a new connected PostgreSQL client.
 */
export async function getClient(): Promise<Client> {
  const client = new Client({
    host: config.pg.host,
    port: config.pg.port,
    user: config.pg.user,
    password: config.pg.password,
    database: config.pg.database,
  });
  await client.connect();
  return client;
}
