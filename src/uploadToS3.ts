import AWS from "aws-sdk";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import { createLogger, format, transports } from "winston";

// Load environment variables from .env file (assumed one level up)
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });
dotenv.config();

// Get the current date in YYYY-MM-DD format for log file naming.
const currentDate = new Date().toISOString().slice(0, 10);

// Set up Winston logger for both console and file logging.
const logger = createLogger({
  level: "info",
  format: format.combine(format.timestamp(), format.json()),
  transports: [
    new transports.Console(),
    new transports.File({
      // Using a date-stamped filename: app-error-YYYY-MM-DD.log
      filename: path.join(
        __dirname,
        "..",
        "logs",
        `app-error-${currentDate}.log`
      ),
    }),
  ],
});

// Validate required environment variables.
const { AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME } =
  process.env;
if (
  !AWS_ACCESS_KEY_ID ||
  !AWS_SECRET_ACCESS_KEY ||
  !AWS_REGION ||
  !S3_BUCKET_NAME
) {
  logger.error(
    "Missing one or more required environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, S3_BUCKET_NAME"
  );
  process.exit(1);
}

// Set up AWS S3 client using environment variables.
const s3 = new AWS.S3({
  accessKeyId: AWS_ACCESS_KEY_ID,
  secretAccessKey: AWS_SECRET_ACCESS_KEY,
  region: AWS_REGION,
});

const bucketName = S3_BUCKET_NAME;

// Helper function to upload a file to S3.
async function uploadFileToS3(filePath: string, s3Key: string): Promise<void> {
  try {
    // Validate the file exists.
    if (!fs.existsSync(filePath)) {
      logger.error(`File not found: ${filePath}`);
      return;
    }

    const fileContent = fs.readFileSync(filePath);
    const params = {
      Bucket: bucketName,
      Key: s3Key,
      Body: fileContent,
      ContentType: "text/csv",
    };

    const result = await s3.upload(params).promise();
    logger.info(
      `File "${s3Key}" uploaded successfully to S3: ${result.Location}`
    );
  } catch (error) {
    logger.error(`Error uploading file "${s3Key}": ${error}`);
  }
}

// Define local paths for the transformed CSV files.
const transformedTracksPath = path.join(
  __dirname,
  "..",
  "data",
  "transformedTracks.csv"
);
const transformedArtistsPath = path.join(
  __dirname,
  "..",
  "data",
  "transformedArtists.csv"
);

// Run uploads for both files.
async function runUploads() {
  await uploadFileToS3(transformedTracksPath, "transformedTracks.csv");
  await uploadFileToS3(transformedArtistsPath, "transformedArtists.csv");
}

runUploads().catch((err) => {
  logger.error(`Unhandled error: ${err}`);
});
