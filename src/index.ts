import path from "path";
import { logger } from "./utils/logger";
import { config } from "./config/config";
import {
  FilterTransform,
  ArtistFilterTransform,
  processCSV,
} from "./transformer";

export async function runTransformation(): Promise<void> {
  try {
    // Process tracks.csv
    const trackFilter = new FilterTransform({
      minDuration: config.transform.minDuration,
    });
    await processCSV(
      config.files.tracksInput,
      config.files.tracksOutput,
      trackFilter
    );
    logger.info(`Transformed tracks saved to: ${config.files.tracksOutput}`);

    // Process artists.csv, filtering using unique artist IDs collected from tracks.
    const artistFilter = new ArtistFilterTransform(trackFilter.uniqueArtistIds);
    await processCSV(
      config.files.artistsInput,
      config.files.artistsOutput,
      artistFilter
    );
    logger.info(`Transformed artists saved to: ${config.files.artistsOutput}`);
  } catch (error) {
    logger.error(`Error running transformation: ${error}`);
  }
}

runTransformation().catch((err) => {
  logger.error(`Unhandled error: ${err}`);
});
