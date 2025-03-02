// uploadToS3.ts
import AWS from "aws-sdk";
import path from "path";
import { promises as fsPromises } from "fs";
import { logger } from "./utils/logger";
import { config } from "./config/config";

// Set up AWS S3 client using environment variables from config.
const s3 = new AWS.S3({
  accessKeyId: config.aws.accessKeyId,
  secretAccessKey: config.aws.secretAccessKey,
  region: config.aws.region,
});

const bucketName = config.aws.bucketName;

/**
 * Uploads a file to S3 with a simple retry mechanism.
 * @param filePath Local file path to upload.
 * @param s3Key The key (file name) under which the file is saved in S3.
 * @param retries Number of retry attempts (default is 3).
 */
export async function uploadFileToS3(
  filePath: string,
  s3Key: string,
  retries = 3
): Promise<void> {
  // Check if the file exists.
  try {
    await fsPromises.access(filePath);
  } catch {
    logger.error(`File not found: ${filePath}`);
    return;
  }

  let fileContent: Buffer;
  try {
    fileContent = await fsPromises.readFile(filePath);
  } catch (readError) {
    logger.error(`Error reading file "${filePath}": ${readError}`);
    return;
  }

  const params = {
    Bucket: bucketName,
    Key: s3Key,
    Body: fileContent,
    ContentType: "text/csv",
  };

  // Attempt to upload with retries.
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const result = await s3.upload(params).promise();
      logger.info(
        `File "${s3Key}" uploaded successfully to S3: ${result.Location}`
      );
      return; // Exit after a successful upload.
    } catch (error) {
      logger.error(
        `Attempt ${attempt} - Error uploading file "${s3Key}": ${error}`
      );
      if (attempt === retries) {
        logger.error(`Failed to upload "${s3Key}" after ${retries} attempts.`);
      }
    }
  }
}

// Define local paths for the transformed CSV files.
export const transformedTracksPath = path.join(
  __dirname,
  "..",
  "data",
  "transformedTracks.csv"
);
export const transformedArtistsPath = path.join(
  __dirname,
  "..",
  "data",
  "transformedArtists.csv"
);

/**
 * Runs uploads for both files concurrently.
 */
export async function runUploads() {
  try {
    await Promise.all([
      uploadFileToS3(transformedTracksPath, "transformedTracks.csv"),
      uploadFileToS3(transformedArtistsPath, "transformedArtists.csv"),
    ]);
  } catch (err) {
    logger.error(`Unhandled error during uploads: ${err}`);
  }
}

// Only run uploads if this module is executed directly.
if (require.main === module) {
  runUploads();
}
