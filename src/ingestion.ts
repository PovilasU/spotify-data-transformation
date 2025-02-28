import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";
import { Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";

// Define interfaces for input track rows and output artist IDs.
interface TrackRow {
  name: string;
  duration_ms: string;
  id_artists: string;
  release_date?: string;
  danceability?: string; // Expected to be a numeric string.
  [key: string]: any;
}

interface ArtistId {
  id: string;
}

/**
 * Parses a release date string into year, month, and day.
 * Supports formats: "YYYY-MM-DD", "DD/MM/YYYY", and "YYYY".
 * Returns an object with properties {year, month, day}.
 */
function parseReleaseDate(dateStr: string): {
  year: string;
  month: string;
  day: string;
} {
  // Remove non-ASCII characters (BOM, zero-width spaces) and trim.
  let cleaned = dateStr.replace(/[^\x20-\x7E]+/g, "").trim();

  // Format: "YYYY-MM-DD" (e.g., "1929-01-12")
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    const [year, month, day] = cleaned.split("-");
    return { year, month, day };
  }

  // Format: "DD/MM/YYYY" (e.g., "22/02/1929")
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(cleaned)) {
    const [dd, mm, yyyy] = cleaned.split("/");
    return { year: yyyy, month: mm, day: dd };
  }

  // Format: "YYYY"
  if (/^\d{4}$/.test(cleaned)) {
    return { year: cleaned, month: "", day: "" };
  }

  // Unknown format
  return { year: "", month: "", day: "" };
}

/**
 * Transforms a numeric danceability value into a categorical label.
 * [0, 0.5)  => "Low"
 * [0.5, 0.6] => "Medium"
 * (0.6, 1]  => "High"
 */
function transformDanceability(value: string | undefined): string {
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
 * - Transforms danceability into a string label.
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

      // Validate: skip if name is missing or duration is not a valid number or is less than minimum.
      if (!name || isNaN(duration) || duration < this.minDuration) {
        return callback(); // Skip this track.
      }

      // Parse id_artists from string (e.g., "['id1','id2']") into an array.
      let idArtistsArray: string[] = [];
      if (track.id_artists) {
        try {
          const normalized = track.id_artists.replace(/'/g, '"');
          idArtistsArray = JSON.parse(normalized);
        } catch (err) {
          console.error(`Error parsing id_artists for track "${name}":`, err);
          idArtistsArray = [track.id_artists];
        }
      }
      idArtistsArray.forEach((id) => {
        const trimmed = id.trim();
        if (trimmed) {
          this.uniqueArtistIds.add(trimmed);
        }
      });

      // Parse the release_date into year, month, and day.
      const { year, month, day } = parseReleaseDate(track.release_date || "");
      track.release_year = year;
      track.release_month = month;
      track.release_day = day;

      // Transform danceability into a categorical label.
      track.danceability_level = transformDanceability(track.danceability);

      // Pass the valid, transformed track onward.
      callback(null, track);
    } catch (error) {
      // Log any error during transformation and skip this record.
      console.error("Error in _transform:", error);
      callback();
    }
  }
}

/**
 * Main function: sets up the streaming pipeline with error handling and progress reporting.
 */
async function runTransformation(): Promise<void> {
  // Configuration parameters.
  const config = {
    inputFile: path.join(__dirname, "../data/tracks.csv"),
    outputTracksFile: path.join(__dirname, "../data/transformedTracks.csv"),
    outputArtistIdsFile: path.join(__dirname, "../data/uniqueArtistIds.csv"),
    minDuration: 60000, // 1 minute (in milliseconds)
  };

  try {
    // Set up a progress bar based on the input file size.
    const { size: totalBytes } = fs.statSync(config.inputFile);
    const progressBar = new cliProgress.SingleBar(
      {},
      cliProgress.Presets.shades_classic
    );
    progressBar.start(totalBytes, 0);

    // Create a read stream for the input CSV.
    const readStream = fs.createReadStream(config.inputFile);
    readStream.on("data", (chunk: Buffer | string) => {
      const length =
        typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
      progressBar.increment(length);
    });

    // Instantiate the custom transform stream.
    const filterTransform = new FilterTransform({
      minDuration: config.minDuration,
    });

    // Create a CSV stringifier for output.
    const csvStringifier = stringify({ header: true });
    const tracksWriteStream = fs.createWriteStream(config.outputTracksFile);

    // Chain the streams using pipeline for robust error handling.
    await pipeline(
      readStream,
      csvParser(),
      filterTransform,
      csvStringifier,
      tracksWriteStream
    );
    progressBar.stop();
    console.log(
      `Filtered and transformed tracks saved to: ${config.outputTracksFile}`
    );

    // Write the unique artist IDs (collected only from valid tracks) to a separate CSV.
    const uniqueArtistIdsArray: ArtistId[] = Array.from(
      filterTransform.uniqueArtistIds
    ).map((id) => ({ id }));
    stringify(uniqueArtistIdsArray, { header: true }, (err, output) => {
      if (err) {
        console.error("Error stringifying artist IDs:", err);
        return;
      }
      fs.writeFileSync(config.outputArtistIdsFile, output);
      console.log(`Unique artist IDs saved to: ${config.outputArtistIdsFile}`);
    });
  } catch (error) {
    console.error("Error in runTransformation:", error);
  }
}

// Run the transformation and catch any top-level errors.
runTransformation().catch((err) => {
  console.error("Top-level error running transformation:", err);
});
