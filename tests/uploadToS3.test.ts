// tests/uploadToS3.test.ts

// Ensure that LOCAL_TEST is "false" before any module is imported.
process.env.LOCAL_TEST = "false";

// Declare our mocks using var to avoid hoisting issues.
var mockUploadPromise = jest.fn();
var mockUpload = jest.fn(() => ({ promise: mockUploadPromise }));

// MOCK AWS SDK BEFORE importing any modules that use it.
jest.mock("aws-sdk", () => {
  return {
    S3: jest.fn().mockImplementation(() => ({
      upload: mockUpload,
    })),
  };
});

// Now import modules that depend on AWS.
import path from "path";
import { promises as fsPromises } from "fs";
import { logger } from "../src/utils/logger";
import { uploadFileToS3, runUploads } from "../src/uploadToS3";
import AWS from "aws-sdk";

describe("uploadFileToS3", () => {
  const testFilePath = path.join(__dirname, "test.csv");
  const testS3Key = "test.csv";
  const fileContent = Buffer.from("sample,data");

  beforeEach(() => {
    // Use clearAllMocks so our mock implementations remain.
    jest.clearAllMocks();
  });

  it("should log an error if the file does not exist", async () => {
    jest
      .spyOn(fsPromises, "access")
      .mockRejectedValue(new Error("File not found"));
    const loggerErrorSpy = jest
      .spyOn(logger, "error")
      .mockImplementation(() => logger);

    await uploadFileToS3(testFilePath, testS3Key);
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(`File not found: ${testFilePath}`)
    );
  });

  it("should log an error if reading the file fails", async () => {
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    jest
      .spyOn(fsPromises, "readFile")
      .mockRejectedValue(new Error("Read error"));
    const loggerErrorSpy = jest
      .spyOn(logger, "error")
      .mockImplementation(() => logger);

    await uploadFileToS3(testFilePath, testS3Key);
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(`Error reading file "${testFilePath}":`)
    );
  });

  it("should upload the file successfully", async () => {
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    jest.spyOn(fsPromises, "readFile").mockResolvedValue(fileContent);
    // Simulate a successful upload by resolving the promise.
    mockUploadPromise.mockResolvedValue({
      Location: "https://s3.amazonaws.com/test-bucket/test.csv",
    });
    const loggerInfoSpy = jest
      .spyOn(logger, "info")
      .mockImplementation(() => logger);

    await uploadFileToS3(testFilePath, testS3Key);
    expect(mockUpload).toHaveBeenCalled();
    expect(loggerInfoSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `File "${testS3Key}" uploaded successfully to S3:`
      )
    );
  });

  it("should retry and eventually fail if S3 upload fails", async () => {
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    jest.spyOn(fsPromises, "readFile").mockResolvedValue(fileContent);
    // Simulate upload failures by rejecting the promise.
    mockUploadPromise.mockRejectedValue(new Error("Upload error"));
    const loggerErrorSpy = jest
      .spyOn(logger, "error")
      .mockImplementation(() => logger);

    // Use a retry count of 2.
    await uploadFileToS3(testFilePath, testS3Key, 2);

    // Verify that our S3.upload method (i.e. mockUpload) was called twice.
    expect(mockUpload).toHaveBeenCalledTimes(2);
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `Attempt 1 - Error uploading file "${testS3Key}":`
      )
    );
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `Attempt 2 - Error uploading file "${testS3Key}":`
      )
    );
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `Failed to upload "${testS3Key}" after 2 attempts.`
      )
    );
  });
});

describe("runUploads", () => {
  const dummyContent = Buffer.from("dummy,data");

  beforeEach(() => {
    jest.clearAllMocks();
    // Stub fsPromises methods for both transformed files.
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    jest.spyOn(fsPromises, "readFile").mockResolvedValue(dummyContent);
    // Simulate successful upload.
    mockUploadPromise.mockResolvedValue({
      Location: "https://s3.amazonaws.com/test-bucket/dummy.csv",
    });
    jest.spyOn(logger, "info").mockImplementation(() => logger);
  });

  it("should attempt to upload both transformed files concurrently", async () => {
    await runUploads();
    // We expect that S3.upload (mockUpload) is called twice.
    expect(mockUpload).toHaveBeenCalledTimes(2);
  });

  it("should log an error if one of the uploads fails", async () => {
    // Simulate one upload succeeds and the other fails.
    mockUploadPromise
      .mockResolvedValueOnce({
        Location: "https://s3.amazonaws.com/test-bucket/dummy.csv",
      })
      .mockRejectedValueOnce(new Error("Upload error"));
    const loggerErrorSpy = jest
      .spyOn(logger, "error")
      .mockImplementation(() => logger);
    await runUploads();
    // Since uploadFileToS3 swallows its errors, runUploads will not log "Unhandled error" but will log the attempt error.
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `Attempt 1 - Error uploading file "transformedArtists.csv":`
      )
    );
  });
});
