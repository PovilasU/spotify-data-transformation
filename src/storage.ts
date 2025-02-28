// AWS S3 storage script
import * as AWS from "aws-sdk";
import * as fs from "fs";
import * as path from "path";
import * as dotenv from "dotenv";

dotenv.config();

const s3 = new AWS.S3();
const bucketName = process.env.AWS_BUCKET_NAME!;

export class Storage {
  public async uploadData(fileName: string, data: any): Promise<void> {
    const filePath = path.join(__dirname, "../data", fileName);
    fs.writeFileSync(filePath, JSON.stringify(data));

    const fileContent = fs.readFileSync(filePath);

    const params = {
      Bucket: bucketName,
      Key: fileName,
      Body: fileContent,
    };

    try {
      await s3.upload(params).promise();
      console.log(`Data uploaded to S3: ${fileName}`);
    } catch (error) {
      console.error("Error uploading to S3:", error);
    }
  }
}
