import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";
import { Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";

// Define interfaces for type safety
interface TrackRow {
  name: string;
  duration_ms: string;
  id_artists: string;
  release_date?: string;
  danceability?: string; // May be a numeric string
  [key: string]: any;
}

interface ArtistId {
  id: string;
}

// Date parser that handles both "YYYY-MM-DD", "DD/MM/YYYY", and "YYYY" formats.
function parseReleaseDate(dateStr: string): {
  year: string;
  month: string;
  day: string;
} {
  // Remove non-ASCII characters and trim
  let cleaned = dateStr.replace(/[^\x20-\x7E]+/g, "").trim();

  // If format is "YYYY-MM-DD"
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    const [year, month, day] = cleaned.split("-");
    return { year, month, day };
  }

  // If format is "DD/MM/YYYY"
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(cleaned)) {
    const [dd, mm, yyyy] = cleaned.split("/");
    return { year: yyyy, month: mm, day: dd };
  }

  // If format is "YYYY" only
  if (/^\d{4}$/.test(cleaned)) {
    return { year: cleaned, month: "", day: "" };
  }

  return { year: "", month: "", day: "" };
}

// Custom transform stream that filters tracks and applies transformations.
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
  ) {
    const track = chunk as TrackRow;
    const name = track.name?.trim();
    const duration = Number(track.duration_ms);

    // 1. Ignore tracks that have no name or duration less than 1 minute.
    if (!name || duration < this.minDuration) {
      return callback(); // Do not push invalid tracks downstream.
    }

    // 2. Parse id_artists (e.g. "['id1','id2']") into an array and collect unique artist IDs.
    let idArtistsArray: string[] = [];
    if (track.id_artists) {
      try {
        const normalized = track.id_artists.replace(/'/g, '"');
        idArtistsArray = JSON.parse(normalized);
      } catch {
        idArtistsArray = [track.id_artists];
      }
    }
    idArtistsArray.forEach((id) => {
      const trimmed = id.trim();
      if (trimmed) {
        this.uniqueArtistIds.add(trimmed);
      }
    });

    // 3. Parse release_date into separate columns (release_year, release_month, release_day).
    const { year, month, day } = parseReleaseDate(track.release_date || "");
    track.release_year = year;
    track.release_month = month;
    track.release_day = day;

    // 4. Transform danceability into a string label.
    const danceability = Number(track.danceability);
    if (!isNaN(danceability)) {
      if (danceability < 0.5) {
        track.danceability_level = "Low";
      } else if (danceability <= 0.6) {
        track.danceability_level = "Medium";
      } else {
        track.danceability_level = "High";
      }
    } else {
      track.danceability_level = "";
    }

    // Pass the valid and transformed record onward.
    callback(null, track);
  }
}

async function runTransformation() {
  const config = {
    inputFile: path.join(__dirname, "../data/tracks.csv"),
    outputTracksFile: path.join(__dirname, "../data/transformedTracks.csv"),
    outputArtistIdsFile: path.join(__dirname, "../data/uniqueArtistIds.csv"),
    minDuration: 60000, // 1 minute in milliseconds
  };

  // Create a progress bar based on file size.
  const { size: totalBytes } = fs.statSync(config.inputFile);
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(totalBytes, 0);

  // Create read stream and update progress.
  const readStream = fs.createReadStream(config.inputFile);
  readStream.on("data", (chunk: Buffer | string) => {
    const length =
      typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    progressBar.increment(length);
  });

  // Set up transform stream.
  const filterTransform = new FilterTransform({
    minDuration: config.minDuration,
  });

  // Create CSV stringifier for output.
  const csvStringifier = stringify({ header: true });
  const tracksWriteStream = fs.createWriteStream(config.outputTracksFile);

  try {
    // Pipeline: input -> csvParser -> filterTransform -> csvStringifier -> output file
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
    progressBar.stop();
    console.error("Error during pipeline processing:", error);
    return;
  }

  // Write unique artist IDs from filtered tracks.
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
}

// Execute the transformation pipeline.
runTransformation().catch((err) => {
  console.error("Error running transformation:", err);
});
