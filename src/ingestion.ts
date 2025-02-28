import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";
import { Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";

interface TrackRow {
  name: string;
  duration_ms: string;
  id_artists: string;
  release_date?: string;
  [key: string]: any;
}

interface ArtistId {
  id: string;
}

// Updated date parser to handle "YYYY-MM-DD", "DD/MM/YYYY", and "YYYY"
function parseReleaseDate(dateStr: string): {
  year: string;
  month: string;
  day: string;
} {
  // 1) Remove non-ASCII characters (like BOM or zero-width spaces)
  let cleaned = dateStr.replace(/[^\x20-\x7E]+/g, "").trim();

  // 2) If it's "YYYY-MM-DD"
  //    e.g., "1929-01-12" => year="1929", month="01", day="12"
  if (/^\d{4}-\d{2}-\d{2}$/.test(cleaned)) {
    const [year, month, day] = cleaned.split("-");
    return { year, month, day };
  }

  // 3) If it's "DD/MM/YYYY"
  //    e.g., "22/02/1929" => year="1929", month="02", day="22"
  if (/^\d{1,2}\/\d{1,2}\/\d{4}$/.test(cleaned)) {
    const [dd, mm, yyyy] = cleaned.split("/");
    return { year: yyyy, month: mm, day: dd };
  }

  // 4) If it's just "YYYY"
  if (/^\d{4}$/.test(cleaned)) {
    return { year: cleaned, month: "", day: "" };
  }

  // 5) Otherwise, unknown format => empty
  return { year: "", month: "", day: "" };
}

// Transform stream to filter tracks + parse date + collect artist IDs
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

    // Filter: name must be non-empty, duration >= minDuration
    if (!name || duration < this.minDuration) {
      return callback();
    }

    // Parse id_artists (e.g. "['id1','id2']")
    let idArtistsArray: string[] = [];
    if (track.id_artists) {
      try {
        const normalized = track.id_artists.replace(/'/g, '"');
        idArtistsArray = JSON.parse(normalized);
      } catch {
        // Fallback: treat entire string as one ID
        idArtistsArray = [track.id_artists];
      }
    }
    // Collect unique artist IDs
    idArtistsArray.forEach((id) => {
      const trimmed = id.trim();
      if (trimmed) {
        this.uniqueArtistIds.add(trimmed);
      }
    });

    // Parse release_date
    const { year, month, day } = parseReleaseDate(track.release_date || "");
    track.release_year = year;
    track.release_month = month;
    track.release_day = day;

    // Pass this record onward
    callback(null, track);
  }
}

async function runTransformation() {
  const config = {
    inputFile: path.join(__dirname, "../data/tracks.csv"),
    outputTracksFile: path.join(__dirname, "../data/transformedTracks.csv"),
    outputArtistIdsFile: path.join(__dirname, "../data/uniqueArtistIds.csv"),
    minDuration: 60000, // 1 minute
  };

  // Track file size for the progress bar
  const { size: totalBytes } = fs.statSync(config.inputFile);
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(totalBytes, 0);

  const readStream = fs.createReadStream(config.inputFile);
  readStream.on("data", (chunk: Buffer | string) => {
    const length =
      typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    progressBar.increment(length);
  });

  const filterTransform = new FilterTransform({
    minDuration: config.minDuration,
  });
  const csvStringifier = stringify({ header: true });
  const tracksWriteStream = fs.createWriteStream(config.outputTracksFile);

  try {
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

  // Write unique artist IDs
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

runTransformation().catch((err) => {
  console.error("Error running transformation:", err);
});
