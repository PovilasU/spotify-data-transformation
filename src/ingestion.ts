import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";
import { Transform, TransformCallback } from "stream";
import { pipeline } from "stream/promises";

// Define TypeScript interfaces for type safety
interface TrackRow {
  name: string;
  duration_ms: string;
  id_artists: string;
  // Include other columns as needed
  [key: string]: any;
}

interface ArtistId {
  id: string;
}

// Custom transform stream to filter tracks and collect unique artist IDs
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
    const name = track.name;
    const duration = Number(track.duration_ms);

    // Filter criteria: non-empty name and duration_ms at least minDuration
    if (name && name.trim() !== "" && duration >= this.minDuration) {
      // Parse the id_artists field (e.g. "['id1','id2']") into an array
      let idArtistsArray: string[] = [];
      if (track.id_artists) {
        try {
          const normalized = track.id_artists.replace(/'/g, '"');
          idArtistsArray = JSON.parse(normalized);
        } catch (error) {
          // Fallback: treat the entire string as one ID if parsing fails
          idArtistsArray = [track.id_artists];
        }
      }
      // Add each trimmed artist ID to the set of unique IDs
      idArtistsArray.forEach((id) => {
        const trimmed = id.trim();
        if (trimmed) {
          this.uniqueArtistIds.add(trimmed);
        }
      });
      // Pass the record along (without modifying it)
      callback(null, track);
    } else {
      // Filter out the record by not pushing it downstream
      callback();
    }
  }
}

async function runTransformation() {
  // Configurable parameters
  const config = {
    inputFile: path.join(__dirname, "../data/tracks.csv"),
    outputTracksFile: path.join(__dirname, "../data/transformedTracks.csv"),
    outputArtistIdsFile: path.join(__dirname, "../data/uniqueArtistIds.csv"),
    minDuration: 60000, // Minimum duration in ms (1 minute)
  };

  // Get total file size (in bytes) for progress tracking
  const { size: totalBytes } = fs.statSync(config.inputFile);
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(totalBytes, 0);

  // Create a read stream for the input CSV
  const readStream = fs.createReadStream(config.inputFile);
  // Update progress bar based on chunk sizes
  readStream.on("data", (chunk: Buffer | string) => {
    const length =
      typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    progressBar.increment(length);
  });

  // Create an instance of the filter transform stream
  const filterTransform = new FilterTransform({
    minDuration: config.minDuration,
  });

  // Create a CSV stringifier stream (object mode) for writing filtered tracks
  const csvStringifier = stringify({ header: true });
  const tracksWriteStream = fs.createWriteStream(config.outputTracksFile);

  try {
    // Use Node's pipeline (async/await) to process streams robustly
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

  // After the pipeline completes, write unique artist IDs to a separate CSV.
  // Map the unique IDs to objects with property "id"
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

// Run the transformation and catch any errors at the top level
runTransformation().catch((err) => {
  console.error("Error running transformation:", err);
});
