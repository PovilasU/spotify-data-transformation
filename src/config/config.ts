import path from "path";
import dotenv from "dotenv";

// Load environment variables from .env file (assumed one level up)
dotenv.config({ path: path.resolve(__dirname, "..", "..", ".env") });

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
};
