import {
  loadCSVIntoTable,
  ensureDatabaseExists,
} from "../src/loadFromS3ToPostgres";
import { logger } from "../src/logger";
import { Client } from "pg";
import * as fs from "fs";
import { parse } from "csv-parse/sync";

// Mock the specific fs functions we need
jest.mock("fs", () => {
  const actualFs = jest.requireActual("fs");
  return {
    ...actualFs,
    promises: {
      ...actualFs.promises,
      readFile: jest.fn(),
    },
    existsSync: jest.fn().mockReturnValue(true),
  };
});

// Mock cli-progress
jest.mock("cli-progress", () => {
  return {
    SingleBar: jest.fn().mockImplementation(() => ({
      start: jest.fn(),
      increment: jest.fn(),
      stop: jest.fn(),
    })),
  };
});

// Mock pg Client
const mockQuery = jest.fn();
const mockClient = {
  connect: jest.fn(),
  query: mockQuery,
  end: jest.fn(),
  release: jest.fn(),
};
jest.mock("pg", () => {
  return {
    Client: jest.fn(() => mockClient),
    Pool: jest.fn(() => ({
      connect: jest.fn(() => mockClient),
    })),
  };
});

// Spy on logger
jest.spyOn(logger, "info").mockImplementation(() => logger);
jest.spyOn(logger, "error").mockImplementation(() => logger);

// Mock process.exit
const mockExit = jest.spyOn(process, "exit").mockImplementation((code) => {
  throw new Error(`process.exit: ${code}`);
});

describe("loadCSVIntoTable", () => {
  beforeEach(() => {
    jest.clearAllMocks();
    process.env.LOCAL_TEST = "true"; // Ensure localTest is true for all tests
  });

  it("should create table and insert records for a valid CSV", async () => {
    const sampleCSV = "id,name\n1,Test Artist";
    jest.spyOn(fs.promises, "readFile").mockResolvedValue(sampleCSV);

    mockQuery.mockResolvedValueOnce({}); // For create table
    mockQuery.mockResolvedValueOnce({}); // For insert (1 record)

    await loadCSVIntoTable("transformedArtists.csv", "artists");

    expect(mockQuery).toHaveBeenCalledTimes(2);
    expect(logger.info).toHaveBeenCalledWith(
      expect.stringContaining('Loaded 1 rows into table "artists".')
    );
  });

  it("should log an error if CSV file is empty", async () => {
    jest.spyOn(fs.promises, "readFile").mockResolvedValue("");

    await loadCSVIntoTable("transformedArtists.csv", "artists");

    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining('CSV file for table "artists" is empty.')
    );
  });

  it("should log an error if required headers are missing", async () => {
    const sampleCSV = "name\nTest Artist";
    jest.spyOn(fs.promises, "readFile").mockResolvedValue(sampleCSV);

    await expect(
      loadCSVIntoTable("transformedArtists.csv", "artists")
    ).rejects.toThrow('Missing required CSV headers for table "artists": id');
  });

  it("should log an error if there is an error during CSV parsing", async () => {
    const invalidCSV = "id,name\n1,Test Artist\ninvalid,line";
    jest.spyOn(fs.promises, "readFile").mockResolvedValue(invalidCSV);

    await expect(
      loadCSVIntoTable("transformedArtists.csv", "artists")
    ).rejects.toThrow('Error parsing CSV file for table "artists"');
  });

  it("should log an error if there is an error during database connection", async () => {
    mockClient.connect.mockRejectedValueOnce(new Error("Connection error"));

    await expect(
      loadCSVIntoTable("transformedArtists.csv", "artists")
    ).rejects.toThrow(
      'Error during CSV load into table "artists": Error: Connection error'
    );
  });

  it("should handle process.exit when database does not exist", async () => {
    mockQuery.mockRejectedValueOnce(new Error("Database does not exist"));

    await expect(ensureDatabaseExists("nonexistent_db")).rejects.toThrow(
      "process.exit: 1"
    );

    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining(
        "Error ensuring database exists: Error: Database does not exist"
      )
    );
  });
});
