import * as fs from "fs";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";
import { Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";
import { logger } from "./logger";
import { config } from "./config";

// Define interfaces for input track rows and artist rows.
export interface TrackRow {
  name: string;
  duration_ms: string;
  id_artists: string;
  release_date?: string;
  danceability?: string; // Expected to be a numeric string.
  [key: string]: any;
}

export interface ArtistRow {
  id: string;
  [key: string]: any;
}

/**
 * Parses a release date string into year, month, and day.
 * Supports formats: "YYYY-MM-DD", "DD/MM/YYYY", and "YYYY".
 */
export function parseReleaseDate(dateStr: string): {
  year: string;
  month: string;
  day: string;
} {
  let cleaned = dateStr.replace(/[^\x20-\x7E]+/g, "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    const [year, month, day] = cleaned.split("-");
    return { year, month, day };
  }
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(cleaned)) {
    const [dd, mm, yyyy] = cleaned.split("/");
    return { year: yyyy, month: mm, day: dd };
  }
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
export function transformDanceability(value: string | undefined): string {
  if (!value) return "";
  const num = Number(value);
  if (isNaN(num)) return "";
  if (num < 0.5) return "Low";
  if (num <= 0.6) return "Medium";
  if (num <= 1) return "High";
  return "";
}

/**
 * Custom transform stream for processing tracks.
 * - Filters out tracks with no name or duration less than the minimum.
 * - Parses the id_artists field and collects unique artist IDs.
 * - Splits release_date into release_year, release_month, and release_day.
 * - Transforms danceability into a categorical label.
 */
export class FilterTransform extends Transform {
  public uniqueArtistIds: Set<string> = new Set();
  private minDuration: number;

  constructor(options: { minDuration: number }) {
    super({ objectMode: true });
    console.log("Minimum duration:", options.minDuration);
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
      if (!name || isNaN(duration) || duration < this.minDuration) {
        return callback();
      }
      let idArtistsArray: string[] = [];
      if (track.id_artists) {
        try {
          const normalized = track.id_artists.replace(/'/g, '"');
          idArtistsArray = JSON.parse(normalized);
        } catch (err) {
          logger.error(`Error parsing id_artists for track "${name}": ${err}`);
          idArtistsArray = [track.id_artists];
        }
      }
      idArtistsArray.forEach((id) => {
        const trimmed = id.trim();
        if (trimmed) {
          this.uniqueArtistIds.add(trimmed);
        }
      });
      const { year, month, day } = parseReleaseDate(track.release_date || "");
      track.release_year = year;
      track.release_month = month;
      track.release_day = day;
      track.danceability_level = transformDanceability(track.danceability);
      callback(null, track);
    } catch (error) {
      logger.error(`Error in FilterTransform: ${error}`);
      callback();
    }
  }
}

/**
 * Custom transform stream for filtering artists.
 * Only passes through artists whose id is in the allowed set.
 */
export class ArtistFilterTransform extends Transform {
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
        callback();
      }
    } catch (error) {
      logger.error(`Error in ArtistFilterTransform: ${error}`);
      callback();
    }
  }
}

/**
 * Processes a CSV file, transforming its content and writing the output to a new CSV file.
 * @param inputFile Path to input CSV.
 * @param outputFile Path to output CSV.
 * @param transformStream The transform stream to apply.
 */
export async function processCSV(
  inputFile: string,
  outputFile: string,
  transformStream: Transform
): Promise<void> {
  const totalBytes = fs.statSync(inputFile).size;
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(totalBytes, 0);

  const readStream = fs.createReadStream(inputFile);
  readStream.on("data", (chunk: Buffer | string) => {
    const length =
      typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    progressBar.increment(length);
  });
  const csvStringifier = stringify({ header: true });
  const writeStream = fs.createWriteStream(outputFile);

  await pipeline(
    readStream,
    csvParser(),
    transformStream,
    csvStringifier,
    writeStream
  );
  progressBar.stop();
}
