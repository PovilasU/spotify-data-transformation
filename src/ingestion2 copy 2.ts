import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import { stringify } from "csv-stringify";
import cliProgress from "cli-progress";

async function transformCSV(): Promise<void> {
  const inputFilePath = path.join(__dirname, "../data/tracks.csv");
  const outputTracksPath = path.join(
    __dirname,
    "../data/transformedTracks.csv"
  );
  const outputArtistIdsPath = path.join(
    __dirname,
    "../data/uniqueArtistIds.csv"
  );

  // Get the file size for progress tracking
  const { size: totalBytes } = fs.statSync(inputFilePath);

  // Create a progress bar based on total bytes
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(totalBytes, 0);

  // Set to accumulate unique artist IDs
  const uniqueArtistIds = new Set<string>();

  // Create a streaming CSV stringifier for writing filtered tracks
  const tracksStringifier = stringify({ header: true });
  const tracksWriteStream = fs.createWriteStream(outputTracksPath);
  tracksStringifier.pipe(tracksWriteStream);

  // Create a read stream for the input CSV file
  const readStream = fs.createReadStream(inputFilePath);

  // Update progress bar based on chunk sizes
  readStream.on("data", (chunk: Buffer | string) => {
    const length =
      typeof chunk === "string" ? Buffer.byteLength(chunk) : chunk.length;
    progressBar.increment(length);
  });

  // Pipe the read stream through csv-parser for record-by-record processing
  readStream
    .pipe(csvParser())
    .on("data", (data) => {
      // Filtering criteria:
      // 1) Valid non-empty name
      // 2) duration_ms is at least 60000 (1 minute)
      const hasValidName = data.name && data.name.trim() !== "";
      const hasValidDuration =
        data.duration_ms && Number(data.duration_ms) >= 60000;

      if (hasValidName && hasValidDuration) {
        // Parse the "id_artists" field (which is a string like "['id1','id2']") into an array
        let idArtistsArray: string[] = [];
        if (data.id_artists) {
          try {
            const normalized = data.id_artists.replace(/'/g, '"');
            idArtistsArray = JSON.parse(normalized);
          } catch (error) {
            // Fallback: treat the entire string as one ID if parsing fails
            idArtistsArray = [data.id_artists];
          }
        }

        // Add each trimmed artist ID to the unique set
        idArtistsArray.forEach((id) => {
          const trimmedId = id.trim();
          if (trimmedId) {
            uniqueArtistIds.add(trimmedId);
          }
        });

        // Write the record to the tracks output stream.
        // The original "id_artists" field is kept as is.
        tracksStringifier.write(data);
      }
    })
    .on("end", () => {
      // Close the CSV stringifier stream
      tracksStringifier.end();
      progressBar.stop();
      console.log(`Filtered tracks saved to: ${outputTracksPath}`);

      // Prepare unique artist IDs for output (each as an object)
      const artistIdsArray = Array.from(uniqueArtistIds).map((id) => ({
        artist_id: id,
      }));
      // Convert unique artist IDs to CSV format with headers
      stringify(artistIdsArray, { header: true }, (err, output) => {
        if (err) {
          console.error("Error stringifying artist IDs:", err);
          return;
        }
        fs.writeFileSync(outputArtistIdsPath, output);
        console.log(`Unique artist IDs saved to: ${outputArtistIdsPath}`);
      });
    })
    .on("error", (error) => {
      progressBar.stop();
      console.error("Error processing CSV:", error);
    });
}

transformCSV();
