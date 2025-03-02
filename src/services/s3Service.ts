// src/services/s3Service.ts
import AWS from "aws-sdk";
import { config } from "../config/config";
import { logger } from "../utils/logger";
import fs from "fs";
import path from "path";

// Set up AWS S3 client
const s3 = new AWS.S3({
  accessKeyId: config.aws.accessKeyId,
  secretAccessKey: config.aws.secretAccessKey,
  region: config.aws.region,
});

const tracksFileKey = "transformedTracks.csv";
const artistsFileKey = "transformedArtists.csv";

/**
 * Download CSV file from S3 and return its content as a string.
 * @param s3Key - The S3 key of the file to download.
 */
export async function downloadCSVFromS3(s3Key: string): Promise<string> {
  try {
    // Check if LOCAL_TEST environment variable is explicitly set to "true".
    if (process.env.LOCAL_TEST === "true") {
      let filePath: string;
      if (s3Key === tracksFileKey) {
        filePath = path.join(
          __dirname,
          "..",
          "..",
          "data",
          "transformedTracks.csv"
        );
      } else if (s3Key === artistsFileKey) {
        filePath = path.join(
          __dirname,
          "..",
          "..",
          "data",
          "transformedArtists.csv"
        );
      } else {
        throw new Error(`Local file for key "${s3Key}" is not configured.`);
      }
      logger.info(`Reading CSV from local file: ${filePath}`);
      return await fs.promises.readFile(filePath, "utf-8");
    } else if (process.env.LOCAL_TEST === "false") {
      // If LOCAL_TEST is explicitly "false", download from S3.
      logger.info(`Downloading CSV from S3 key: ${s3Key}`);
      const params: AWS.S3.Types.GetObjectRequest = {
        Bucket: config.aws.bucketName,
        Key: s3Key,
      };
      const data = await s3.getObject(params).promise();
      if (!data.Body) {
        throw new Error("No file content from S3");
      }
      return data.Body.toString();
    } else {
      // If LOCAL_TEST is not set to "true" or "false", default to S3.
      logger.info(
        `LOCAL_TEST environment variable not set or invalid. Downloading CSV from S3 key: ${s3Key}`
      );
      const params: AWS.S3.Types.GetObjectRequest = {
        Bucket: config.aws.bucketName,
        Key: s3Key,
      };
      const data = await s3.getObject(params).promise();
      if (!data.Body) {
        throw new Error("No file content from S3");
      }
      return data.Body.toString();
    }
  } catch (err) {
    logger.error(`Error downloading CSV for key "${s3Key}": ${err}`);
    throw err;
  }
}
