import { safeConvert } from "../src/loadS3test";
import fs from "fs";
import AWS from "aws-sdk";

import dotenv from "dotenv";
import { parse } from "csv-parse/sync";
import cliProgress from "cli-progress";
import { logger } from "../src/utils/logger";
import path from "path";

// --- Load environment variables and validate configuration ---
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

// describe("safeConvert", () => {
//   test("should convert a numeric string to integer", () => {
//     expect(safeConvert("42", "integer")).toBe(42);
//   });

//   test("should return null for empty string", () => {
//     expect(safeConvert("", "text")).toBeNull();
//   });

//   test("should convert 'true' to boolean true", () => {
//     expect(safeConvert("true", "boolean")).toBe(true);
//   });
// });

describe("downloadCSV", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  test("reads from local file when LOCAL_TEST is true", async () => {
    process.env.LOCAL_TEST = "true";
    const csv = await downloadCSV("../data/transformedTracks.csv");
    expect(fs.promises.readFile).toHaveBeenCalled();
    expect(csv).toContain("id,name");
  });

  test("downloads from S3 when LOCAL_TEST is not true", async () => {
    process.env.LOCAL_TEST = "false";
    const s3 = new AWS.S3();
    const csv = await downloadCSV("../data/transformedTracks.csv");
    expect(s3.getObject).toHaveBeenCalled();
    expect(csv).toContain("id,name");
  });
});
async function downloadCSV(fileName: string): Promise<string> {
  if (process.env.LOCAL_TEST === "true") {
    return fs.promises.readFile(fileName, "utf8");
  } else {
    const s3 = new AWS.S3();
    const params = {
      Bucket: process.env.S3_BUCKET_NAME!,
      Key: fileName,
    };
    const data = await s3.getObject(params).promise();
    return data.Body!.toString("utf-8");
  }
}
