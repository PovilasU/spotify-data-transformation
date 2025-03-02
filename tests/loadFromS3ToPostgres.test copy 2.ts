// tests/loadFromS3ToPostgres.test.ts

import { logger } from "../src/utils/logger";
import { config } from "../src/config/config";

// --- Set up environment variables for testing ---
process.env.AWS_ACCESS_KEY_ID = "dummyAccessKey";
process.env.AWS_SECRET_ACCESS_KEY = "dummySecretKey";
process.env.AWS_REGION = "us-east-1";
process.env.S3_BUCKET_NAME = "dummy-bucket";
process.env.PG_HOST = "localhost";
process.env.PG_PORT = "5432";
process.env.PG_USER = "testuser";
process.env.PG_PASSWORD = "testpassword";
process.env.PG_DATABASE = "testdb";
process.env.LOCAL_TEST = "false"; // Disable local file mode so AWS branch is used

// --- AWS S3 Mock ---
const getObjectMock = jest.fn();
jest.mock("aws-sdk", () => ({
  S3: jest.fn().mockImplementation(() => ({
    getObject: getObjectMock,
  })),
}));

// --- PostgreSQL Client Mocks ---
const mockConnect = jest.fn();
const mockQuery = jest.fn();
const mockEnd = jest.fn();

jest.mock("pg", () => ({
  Client: jest.fn().mockImplementation(() => ({
    connect: mockConnect,
    query: mockQuery,
    end: mockEnd,
  })),
}));

// --- Mock cli-progress ---
// We don’t need actual progress output in tests.
jest.mock("cli-progress", () => ({
  SingleBar: jest.fn().mockImplementation(() => ({
    start: jest.fn(),
    increment: jest.fn(),
    stop: jest.fn(),
  })),
  Presets: { shades_classic: {} },
}));

// Override process.exit so that it throws an error instead of exiting.
jest.spyOn(process, "exit").mockImplementation((code) => {
  throw new Error("process.exit: " + code);
});

// Clear mocks before each test.
beforeEach(() => {
  jest.clearAllMocks();
  mockConnect.mockReset();
  mockQuery.mockReset();
  mockEnd.mockReset();
});

// --- Import the module under test AFTER mocks are set up ---
import { loadCSVIntoTable } from "../src/services/csvLoader";
import { downloadCSVFromS3 } from "../src/services/s3Service";
import { ensureDatabaseExists } from "../src/services/dbService";

// --------------------
// Tests for downloadCSVFromS3
// --------------------
describe("downloadCSVFromS3", () => {
  it("should download CSV file from S3 and return its content as string", async () => {
    const csvContent = "col1,col2\nval1,val2";
    // Set up getObjectMock so that getObject().promise() returns the Buffer.
    getObjectMock.mockReturnValue({
      promise: jest.fn().mockResolvedValue({ Body: Buffer.from(csvContent) }),
    });

    const result = await downloadCSVFromS3("dummyKey.csv");
    expect(result).toEqual(csvContent);
  });

  it("should throw error if no file content is returned", async () => {
    getObjectMock.mockReturnValue({
      promise: jest.fn().mockResolvedValue({ Body: null }),
    });
    await expect(downloadCSVFromS3("dummyKey.csv")).rejects.toThrow(
      "No file content from S3"
    );
  });
});

// --------------------
// Tests for ensureDatabaseExists
// --------------------
describe("ensureDatabaseExists", () => {
  const targetDb = process.env.PG_DATABASE as string;

  beforeEach(() => {
    mockConnect.mockResolvedValue(undefined);
    mockQuery.mockReset();
    mockEnd.mockReset();
  });

  it("should create database if it does not exist", async () => {
    // Simulate that the database does not exist (rowCount: 0).
    mockQuery.mockResolvedValueOnce({ rowCount: 0 });
    // Simulate successful CREATE DATABASE.
    mockQuery.mockResolvedValueOnce({});

    await ensureDatabaseExists(targetDb);
    expect(mockConnect).toHaveBeenCalled();
    expect(mockQuery).toHaveBeenNthCalledWith(
      1,
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [targetDb]
    );
    expect(mockQuery).toHaveBeenNthCalledWith(
      2,
      `CREATE DATABASE "${targetDb}"`
    );
    expect(mockEnd).toHaveBeenCalled();
  });

  it("should not create database if it already exists", async () => {
    // Simulate that the database exists (rowCount: 1).
    mockQuery.mockResolvedValue({ rowCount: 1 });
    await ensureDatabaseExists(targetDb);
    expect(mockQuery).toHaveBeenCalledWith(
      "SELECT 1 FROM pg_database WHERE datname = $1",
      [targetDb]
    );
    expect(mockQuery).toHaveBeenCalledTimes(1);
    expect(mockEnd).toHaveBeenCalled();
  });

  it("should log error and exit if an error occurs", async () => {
    // Simulate error: query returns undefined so that accessing rowCount fails.
    mockQuery.mockResolvedValue(undefined);
    await expect(ensureDatabaseExists(targetDb)).rejects.toThrow(
      /process.exit: 1/
    );
  });
});

// --------------------
// Tests for loadCSVIntoTable
// --------------------
describe("loadCSVIntoTable", () => {
  const dummyCSV = "col1,col2\nval1,val2";
  const tableName = "test_table";

  beforeEach(() => {
    // For ensureDatabaseExists inside loadCSVIntoTable,
    // simulate that the target database already exists.
    mockQuery.mockResolvedValueOnce({ rowCount: 1 });
    // Reset subsequent PG query calls.
    mockConnect.mockResolvedValue(undefined);
    mockQuery.mockReset();
    // For CREATE TABLE and INSERT queries, simulate success.
    mockQuery.mockResolvedValue({});
    mockEnd.mockResolvedValue(undefined);
  });

  it("should load CSV data into the specified table", async () => {
    // Set up getObjectMock to return our dummy CSV.
    getObjectMock.mockReturnValue({
      promise: jest.fn().mockResolvedValue({ Body: Buffer.from(dummyCSV) }),
    });

    await loadCSVIntoTable("dummyKey.csv", tableName);

    // Check that one of the PG query calls contains the CREATE TABLE statement.
    const createTableCall = mockQuery.mock.calls.find((call) =>
      call[0].includes(`CREATE TABLE IF NOT EXISTS ${tableName}`)
    );
    expect(createTableCall).toBeDefined();
    expect(createTableCall[1]).toBeUndefined();

    // Check that one of the PG query calls is an INSERT statement with the expected values.
    const insertCall = mockQuery.mock.calls.find((call) =>
      call[0].includes(`INSERT INTO ${tableName}`)
    );
    expect(insertCall).toBeDefined();
    expect(insertCall[1]).toEqual(expect.arrayContaining(["val1", "val2"]));
  });

  it("should log an error if CSV file is empty", async () => {
    // Set up getObjectMock to return an empty CSV.
    getObjectMock.mockReturnValue({
      promise: jest.fn().mockResolvedValue({ Body: Buffer.from("") }),
    });
    const loggerErrorSpy = jest
      .spyOn(logger, "error")
      .mockImplementation(() => logger);
    await loadCSVIntoTable("dummyKey.csv", tableName);
    expect(loggerErrorSpy).toHaveBeenCalledWith(
      expect.stringContaining(`CSV file for ${tableName} is empty.`)
    );
  });
});
