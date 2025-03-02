import AWS from "aws-sdk";
import { config } from "../config/config";
import { logger } from "../utils/logger";

// Set up AWS S3 client
const s3 = new AWS.S3({
  accessKeyId: config.aws.accessKeyId,
  secretAccessKey: config.aws.secretAccessKey,
  region: config.aws.region,
});

/**
 * Download CSV file from S3 and return its content as a string.
 * @param s3Key - The S3 key of the file to download.
 */
export async function downloadCSVFromS3(s3Key: string): Promise<string> {
  try {
    const params: AWS.S3.Types.GetObjectRequest = {
      Bucket: config.aws.bucketName,
      Key: s3Key,
    };
    const data = await s3.getObject(params).promise();
    if (!data.Body) {
      throw new Error("No file content from S3");
    }
    return data.Body.toString(); // Convert Buffer to string
  } catch (err) {
    logger.error("Error downloading CSV from S3: " + err);
    throw err;
  }
}
