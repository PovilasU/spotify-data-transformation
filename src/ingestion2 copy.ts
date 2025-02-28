import * as fs from "fs";
import * as path from "path";
import csvParser from "csv-parser";
import * as csvStringify from "csv-stringify";
import cliProgress from "cli-progress";

async function transformCSV(): Promise<void> {
  // Define input and output file paths
  const inputFilePath = path.join(__dirname, "../data/tracks.csv");
  const outputTracksPath = path.join(
    __dirname,
    "../data/transformedTracks.csv"
  );
  const outputArtistIdsPath = path.join(
    __dirname,
    "../data/uniqueArtistIds.csv"
  );

  // Read the file content to determine the total number of records for the progress bar.
  // This assumes the file is not extremely large.
  const fileContent = fs.readFileSync(inputFilePath, "utf8");
  const lines = fileContent.split("\n");
  // The first line is assumed to be the header.
  const totalRecords = lines.length - 1;

  // Create a progress bar instance
  const progressBar = new cliProgress.SingleBar(
    {},
    cliProgress.Presets.shades_classic
  );
  progressBar.start(totalRecords, 0);

  // Array to hold filtered records
  const filteredRecords: any[] = [];
  let processedRecords = 0;

  // Create a read stream for the CSV file and parse it
  fs.createReadStream(inputFilePath)
    .pipe(csvParser())
    .on("data", (data) => {
      processedRecords++;
      // Update the progress bar as each record is processed
      progressBar.update(processedRecords);

      // Filter out records that:
      // 1) Have no name (empty or whitespace only)
      // 2) Have duration_ms < 60000 (less than 1 minute)
      const hasValidName = data.name && data.name.trim() !== "";
      const hasValidDuration =
        data.duration_ms && Number(data.duration_ms) >= 60000;

      if (hasValidName && hasValidDuration) {
        // Parse the "id_artists" field, which might look like ['45tIt06XoI0Iio4LBEVpls','3BiJGZsy...']
        let idArtistsArray: string[] = [];

        if (data.id_artists) {
          try {
            // Replace single quotes with double quotes so we can JSON-parse
            const normalizedArtistIds = data.id_artists.replace(/'/g, '"');
            idArtistsArray = JSON.parse(normalizedArtistIds);
          } catch (error) {
            // If parsing fails, you can decide how to handle it:
            // for instance, treat the entire string as one ID
            idArtistsArray = [data.id_artists];
          }
        }

        // Attach the parsed IDs back to the record for later use
        data.id_artists_array = idArtistsArray;
        filteredRecords.push(data);
      }
    })
    .on("end", () => {
      progressBar.stop();

      //
      // 1) Write the filtered tracks to a CSV file
      //
      csvStringify.stringify(
        filteredRecords,
        { header: true },
        (err, tracksOutput) => {
          if (err) {
            console.error("Error during CSV stringification for tracks:", err);
            return;
          }
          fs.writeFileSync(outputTracksPath, tracksOutput);
          console.log(`Filtered tracks saved to: ${outputTracksPath}`);

          //
          // 2) Extract unique artist IDs from filtered tracks
          //
          const uniqueArtistIds = new Set<string>();
          for (const record of filteredRecords) {
            if (Array.isArray(record.id_artists_array)) {
              record.id_artists_array.forEach((artistId: string) => {
                const trimmed = artistId.trim();
                if (trimmed) {
                  uniqueArtistIds.add(trimmed);
                }
              });
            }
          }

          // Convert the Set to an array of objects for CSV
          const artistIdsArray = Array.from(uniqueArtistIds).map((id) => ({
            artist_id: id,
          }));

          // Write unique artist IDs to a CSV file
          csvStringify.stringify(
            artistIdsArray,
            { header: true },
            (err2, artistIdsOutput) => {
              if (err2) {
                console.error(
                  "Error during CSV stringification for artist IDs:",
                  err2
                );
                return;
              }
              fs.writeFileSync(outputArtistIdsPath, artistIdsOutput);
              console.log(`Unique artist IDs saved to: ${outputArtistIdsPath}`);
            }
          );
        }
      );
    })
    .on("error", (error) => {
      progressBar.stop();
      console.error("Error during CSV processing:", error);
    });
}

// Run the transformation
transformCSV();
