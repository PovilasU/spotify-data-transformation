import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";
import { Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";
import { logger } from "./utils/logger";

// Get the current date in YYYY-MM-DD format for log file naming.
const currentDate = new Date().toISOString().slice(0, 10);

// Define interfaces for input track rows and artist rows.
interface TrackRow {
  name: string;
  duration_ms: number;
  id_artists: string[];
  release_date?: string;
  danceability?: number; // Expected to be a numeric string.
  [key: string]: any;
}

interface ArtistRow {
  id: string;
  followers: number;
  genres: string[];
  name: string;
  popularity: number;
}

/**
 * Parses a release date string into year, month, and day.
 * Supports formats: "YYYY-MM-DD", "DD/MM/YYYY", and "YYYY".
 */
function parseReleaseDate(dateStr: string): {
  year: string;
  month: string;
  day: string;
} {
  // Remove non-ASCII characters and trim.
  let cleaned = dateStr.replace(/[^\x20-\x7E]+/g, "").trim();

  // Format: "YYYY-MM-DD"
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    const [year, month, day] = cleaned.split("-");
    return { year, month, day };
  }

  // Format: "DD/MM/YYYY"
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(cleaned)) {
    const [dd, mm, yyyy] = cleaned.split("/");
    return { year: yyyy, month: mm, day: dd };
  }

  // Format: "YYYY"
  if (/^\d{4}$/.test(cleaned)) {
    return { year: cleaned, month: "", day: "" };
  }

  return { year: "", month: "", day: "" };
}

/**
 * Transforms a numeric danceability value into a categorical label.
 * [0, 0.5)  => "Low"
 * [0.5, 0.6] => "Medium"
 * (0.6, 1]  => "High"
 */
function transformDanceability(value: number | undefined): string {
  if (!value) return "";
  const num = Number(value);
  if (isNaN(num)) return "";
  if (num < 0.5) return "Low";
  if (num <= 0.6) return "Medium";
  if (num <= 1) return "High";
  return "";
}

/**
 * Custom transform stream that:
 * - Filters out tracks with no name or duration less than the minimum.
 * - Parses the id_artists field and collects unique artist IDs.
 * - Explodes release_date into separate columns (release_year, release_month, release_day).
 * - Transforms danceability into a categorical label.
 */
class FilterTransform extends Transform {
  public uniqueArtistIds: Set<string> = new Set();
  private minDuration: number;

  constructor(options: { minDuration: number }) {
    super({ objectMode: true });
    this.minDuration = options.minDuration;
  }

  _transform(
    chunk: any,
    _encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    try {
      const track = chunk as TrackRow;
      const name = track.name?.trim();
      const duration = Number(track.duration_ms);

      // Ignore tracks with no name or duration less than the minimum.
      if (!name || isNaN(duration) || duration < this.minDuration) {
        return callback(); // Skip this track.
      }

      // Parse id_artists from string (e.g., "['id1','id2']") into an array.
      let idArtistsArray: string[] = [];
      if (track.id_artists) {
        try {
          idArtistsArray = track.id_artists;
        } catch (err) {
          logger.error(`Error parsing id_artists for track "${name}": ${err}`);
          idArtistsArray = track.id_artists;
        }
      }
      idArtistsArray.forEach((id) => {
        const trimmed = id.trim();
        if (trimmed) {
          this.uniqueArtistIds.add(trimmed);
        }
      });

      // Parse release_date into year, month, and day.
      const { year, month, day } = parseReleaseDate(track.release_date || "");
      track.release_year = year;
      track.release_month = month;
      track.release_day = day;

      // Transform danceability into a categorical label.
      track.danceability_level = transformDanceability(track.danceability);

      // Pass the valid, transformed track onward.
      callback(null, track);
    } catch (error) {
      logger.error(`Error in _transform: ${error}`);
      callback();
    }
  }
}

/**
 * Custom transform stream for filtering artists.
 * Only passes through artists whose id is in the allowed set.
 */
class ArtistFilterTransform extends Transform {
  private allowedArtistIds: Set<string>;

  constructor(allowedArtistIds: Set<string>) {
    super({ objectMode: true });
    this.allowedArtistIds = allowedArtistIds;
  }

  _transform(
    chunk: any,
    _encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    try {
      const artist = chunk as ArtistRow;
      if (this.allowedArtistIds.has(artist.id)) {
        callback(null, artist);
      } else {
        callback(); // Skip artist if not in the allowed set.
      }
    } catch (error) {
      logger.error(`Error in ArtistFilterTransform: ${error}`);
      callback();
    }
  }
}

/**
 * Main function to run the transformation pipelines.
 */
async function runTransformation(): Promise<void> {
  const config = {
    inputFile: path.join(__dirname, "../data/tracks.csv"),
    inputArtistsFile: path.join(__dirname, "../data/artists.csv"),
    outputTracksFile: path.join(__dirname, "../data/transformedTracks.csv"),
    outputArtistsFile: path.join(__dirname, "../data/transformedArtists.csv"),
    minDuration: 60000, // 1 minute in milliseconds.
  };

  let filterTransform: FilterTransform;

  // -------------------------
  // Process tracks.csv to transform tracks and collect unique artist IDs.
  // -------------------------
  try {
    // Set up a progress bar based on the input file size.
    const { size: totalBytes } = fs.statSync(config.inputFile);
    const progressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    progressBar.start(totalBytes, 0);

    // Create a read stream for the input tracks CSV.
    const readStream = fs.createReadStream(config.inputFile);
    readStream.on("data", (chunk: Buffer | string) => {
      const length =
        typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
      progressBar.increment(length);
    });

    // Instantiate the custom transform stream.
    filterTransform = new FilterTransform({ minDuration: config.minDuration });
    const csvStringifier = stringify({ header: true });
    const tracksWriteStream = fs.createWriteStream(config.outputTracksFile);

    // Chain the streams using pipeline.
    await pipeline(
      readStream,
      csvParser(),
      filterTransform,
      csvStringifier,
      tracksWriteStream
    );
    progressBar.stop();
    console.log(`Filtered tracks saved to: ${config.outputTracksFile}`);
  } catch (error) {
    logger.error(`Error during tracks pipeline processing: ${error}`);
    console.error("Error during tracks pipeline processing:", error);
    return;
  }

  // -------------------------
  // Process artists.csv to filter only artists in uniqueArtistIds.
  // -------------------------
  try {
    // Create a read stream for the input artists CSV.
    const artistsReadStream = fs.createReadStream(config.inputArtistsFile);
    // Create a write stream for the transformed artists.
    const artistsWriteStream = fs.createWriteStream(config.outputArtistsFile);
    // Set up CSV stringifier with header.
    const artistsStringifier = stringify({ header: true });

    // Create a transform stream to filter artists.
    const artistFilter = new ArtistFilterTransform(
      filterTransform.uniqueArtistIds
    );

    // Optionally, set up a progress bar for the artists file.
    const { size: artistsTotalBytes } = fs.statSync(config.inputArtistsFile);
    const artistsProgressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    artistsProgressBar.start(artistsTotalBytes, 0);
    artistsReadStream.on("data", (chunk: Buffer | string) => {
      const length =
        typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
      artistsProgressBar.increment(length);
    });

    await pipeline(
      artistsReadStream,
      csvParser(),
      artistFilter,
      artistsStringifier,
      artistsWriteStream
    );
    artistsProgressBar.stop();
    console.log(`Filtered artists saved to: ${config.outputArtistsFile}`);
  } catch (error) {
    logger.error(`Error during artists pipeline processing: ${error}`);
    console.error("Error during artists pipeline processing:", error);
  }
}

// Run the transformation and log any top-level errors.
runTransformation().catch((err) => {
  logger.error(`Top-level error running transformation: ${err}`);
  console.error("Top-level error running transformation:", err);
});
