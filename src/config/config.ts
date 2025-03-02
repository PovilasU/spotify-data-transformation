import path from "path";
import dotenv from "dotenv";

// Load environment variables from .env file (assumed one level up)
dotenv.config({ path: path.resolve(__dirname, "..", "..", ".env") });

// Validate required environment variables.
const requiredEnv = [
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_REGION",
  "S3_BUCKET_NAME",
  "PG_HOST",
  "PG_PORT",
  "PG_USER",
  "PG_PASSWORD",
  "PG_DATABASE",
];

for (const key of requiredEnv) {
  if (!process.env[key]) {
    throw new Error(`Missing required environment variable: ${key}`);
  }
}

export const config = {
  aws: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
    region: process.env.AWS_REGION || "",
    bucketName: process.env.S3_BUCKET_NAME || "",
  },
  postgres: {
    host: process.env.PG_HOST || "localhost",
    port: parseInt(process.env.PG_PORT || "5432"),
    user: process.env.PG_USER || "",
    password: process.env.PG_PASSWORD || "",
    database: process.env.PG_DATABASE || "",
  },
  s3Keys: {
    tracks: "transformedTracks.csv",
    artists: "transformedArtists.csv",
  },
  files: {
    tracksInput: path.join(__dirname, "..", "..", "data", "tracks.csv"),
    artistsInput: path.join(__dirname, "..", "..", "data", "artists.csv"),
    tracksOutput: path.join(
      __dirname,
      "..",
      "..",
      "data",
      "transformedTracks.csv"
    ),
    artistsOutput: path.join(
      __dirname,
      "..",
      "..",
      "data",
      "transformedArtists.csv"
    ),
  },
  transform: {
    minDuration: 60000, // 1 minute in milliseconds.
  },

  //changeto true if dont want to load .csv files from aws s3 but instead  test it with local files from data folder

  localTest: process.env.LOCAL_TEST === "false",
};
