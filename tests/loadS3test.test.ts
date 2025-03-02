import fs from "fs";
import AWS from "aws-sdk";
import dotenv from "dotenv";
import path from "path";

// Load environment variables
dotenv.config({ path: path.resolve(__dirname, "..", ".env") });

// --- Mock AWS S3 for tests ---
jest.mock("aws-sdk", () => {
  return {
    S3: jest.fn(() => ({
      getObject: jest.fn().mockReturnValue({
        promise: jest.fn().mockResolvedValue({
          Body: Buffer.from("id,name\n1,S3Test"),
        }),
      }),
    })),
  };
});

// Helper function to download CSV (as in your code)
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

describe("downloadCSV", () => {
  afterEach(() => {
    jest.clearAllMocks();
  });

  test("reads from local file when LOCAL_TEST is true", async () => {
    process.env.LOCAL_TEST = "true";
    // Resolve the absolute path relative to this test file.
    const filePath = path.resolve(__dirname, "../data/transformedTracks.csv");

    // Spy on fs.promises.readFile to check it's called.
    const readFileSpy = jest.spyOn(fs.promises, "readFile");

    const csv = await downloadCSV(filePath);
    expect(readFileSpy).toHaveBeenCalled();
    expect(csv).toContain("id,name");
  });

  test("downloads from S3 when LOCAL_TEST is not true", async () => {
    process.env.LOCAL_TEST = "false";
    // Spy on the prototype method so that any instance's getObject is tracked.
    const getObjectSpy = jest.spyOn(AWS.S3.prototype, "getObject");
    const csv = await downloadCSV("transformedTracks.csv");
    expect(getObjectSpy).toHaveBeenCalled();
    expect(csv).toContain("id,name");
  });
});
