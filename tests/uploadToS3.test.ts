// tests/uploadToS3.test.ts

// Define external mock functions.
const mockUploadPromise = jest.fn();
const mockUpload = jest.fn(() => ({ promise: mockUploadPromise }));

// Mock the AWS SDK.
jest.mock("aws-sdk", () => {
  return {
    S3: jest.fn().mockImplementation(() => ({
      upload: mockUpload,
    })),
    __mockUploadPromise: mockUploadPromise,
    __mockUpload: mockUpload,
  };
});

// Reset modules so that uploader.ts picks up the AWS mock.
jest.resetModules();

import path from "path";
import { promises as fsPromises } from "fs";
import { logger } from "../src/utils/logger";
import { uploadFileToS3 } from "../src/uploadToS3";
import AWS from "aws-sdk";

// Extract the inline mock promise from our AWS mock.
const { __mockUploadPromise } = AWS as any;

describe("uploadFileToS3", () => {
  const testFilePath = path.join(__dirname, "test.csv");
  const testS3Key = "test.csv";
  const fileContent = Buffer.from("sample,data");

  beforeEach(() => {
    jest.resetAllMocks();
  });

  it("should log an error if the file does not exist", async () => {
    // Simulate file not found.
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
    // Simulate file exists.
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    // Simulate read failure.
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
    // Simulate file exists and read succeeds.
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    jest.spyOn(fsPromises, "readFile").mockResolvedValue(fileContent);
    // Simulate successful S3 upload.
    __mockUploadPromise.mockResolvedValue({
      Location: "https://s3.amazonaws.com/test-bucket/test.csv",
    });
    const loggerInfoSpy = jest
      .spyOn(logger, "info")
      .mockImplementation(() => logger);

    await uploadFileToS3(testFilePath, testS3Key);
    expect(__mockUploadPromise).toHaveBeenCalled(); // Verify that promise was called.
    expect(loggerInfoSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `File "${testS3Key}" uploaded successfully to S3:`
      )
    );
  });

  it("should retry and eventually fail if S3 upload fails", async () => {
    // Simulate file exists and read succeeds.
    jest.spyOn(fsPromises, "access").mockResolvedValue(undefined);
    jest.spyOn(fsPromises, "readFile").mockResolvedValue(fileContent);
    // Simulate S3 upload failure.
    __mockUploadPromise.mockRejectedValue(new Error("Upload error"));
    const loggerErrorSpy = jest
      .spyOn(logger, "error")
      .mockImplementation(() => logger);

    await uploadFileToS3(testFilePath, testS3Key, 2);
    // Expect that __mockUploadPromise was called twice (i.e. two attempts).
    expect(__mockUploadPromise).toHaveBeenCalledTimes(2);
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
