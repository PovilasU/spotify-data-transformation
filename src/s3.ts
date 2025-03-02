import AWS from "aws-sdk";
import fs from "fs";
import path from "path";
import { config } from "./config";
import { logger } from "./logger";

const s3 = new AWS.S3({
  accessKeyId: config.aws.accessKeyId,
  secretAccessKey: config.aws.secretAccessKey,
  region: config.aws.region,
});
const bucketName = config.aws.bucketName;
const tracksFileKey = "transformedTracks.csv";
const artistsFileKey = "transformedArtists.csv";

/**
 * Returns a readable stream from a local file (if localTest is enabled) or from S3.
 * Uses a highWaterMark of 128 KB.
 */
export function getCSVReadStream(s3Key: string): NodeJS.ReadableStream {
  if (config.localTest) {
    let filePath: string;
    if (s3Key === tracksFileKey) {
      filePath = path.join(__dirname, "..", "data", "transformedTracks.csv");
    } else if (s3Key === artistsFileKey) {
      filePath = path.join(__dirname, "..", "data", "transformedArtists.csv");
    } else {
      throw new Error(`Local file for key "${s3Key}" is not configured.`);
    }
    logger.info(`Creating read stream from local file: ${filePath}`);
    return fs.createReadStream(filePath, { highWaterMark: 128 * 1024 });
  } else {
    logger.info(`Creating S3 read stream for key: ${s3Key}`);
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: s3Key,
    };
    return s3.getObject(params).createReadStream();
  }
}

/**
 * Downloads CSV file from S3 and returns its content as a string.
 */
export async function downloadCSVFromS3(s3Key: string): Promise<string> {
  try {
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: bucketName,
      Key: s3Key,
    };
    const data = await s3.getObject(params).promise();
    if (!data.Body) {
      throw new Error("No file content from S3");
    }
    return data.Body.toString();
  } catch (err) {
    logger.error("Error downloading CSV from S3: " + err);
    throw err;
  }
}
