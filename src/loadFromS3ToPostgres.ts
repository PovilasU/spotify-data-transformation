import { loadCSVIntoTable } from "./services/csvLoader";
import { config } from "./config/config";

async function runLoads() {
  await loadCSVIntoTable(config.s3Keys.tracks, "tracks");
  await loadCSVIntoTable(config.s3Keys.artists, "artists");
}

runLoads().catch((err) => {
  console.error("Unhandled error: " + err);
});
