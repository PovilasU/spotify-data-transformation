// tests/config.test.ts
import path from "path";

// Mock dotenv so that it does not load a real .env file during tests.
jest.mock("dotenv", () => ({
  config: jest.fn(() => ({ parsed: {} })),
}));

describe("config module", () => {
  beforeEach(() => {
    // Clear any previously set environment variables.
    jest.resetModules();
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_ACCESS_KEY;
    delete process.env.AWS_REGION;
    delete process.env.S3_BUCKET_NAME;
    delete process.env.PG_HOST;
    delete process.env.PG_PORT;
    delete process.env.PG_USER;
    delete process.env.PG_PASSWORD;
    delete process.env.PG_DATABASE;
  });

  it("should throw an error if a required environment variable is missing", () => {
    // Set all required env variables except one (AWS_ACCESS_KEY_ID is missing).
    process.env.AWS_SECRET_ACCESS_KEY = "secret";
    process.env.AWS_REGION = "us-east-1";
    process.env.S3_BUCKET_NAME = "mybucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "user";
    process.env.PG_PASSWORD = "password";
    process.env.PG_DATABASE = "db";

    // Requiring the module should throw an error due to missing AWS_ACCESS_KEY_ID.
    expect(() => {
      require("../src/config");
    }).toThrow(/Missing required environment variable: AWS_ACCESS_KEY_ID/);
  });

  it("should export a valid config when all required environment variables are set", () => {
    // Set all required environment variables.
    process.env.AWS_ACCESS_KEY_ID = "access";
    process.env.AWS_SECRET_ACCESS_KEY = "secret";
    process.env.AWS_REGION = "us-east-1";
    process.env.S3_BUCKET_NAME = "mybucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "user";
    process.env.PG_PASSWORD = "password";
    process.env.PG_DATABASE = "db";

    const { config } = require("../src/config");
    expect(config).toEqual({
      aws: {
        accessKeyId: "access",
        secretAccessKey: "secret",
        region: "us-east-1",
        bucketName: "mybucket",
      },
      pg: {
        host: "localhost",
        port: 5432,
        user: "user",
        password: "password",
        database: "db",
      },
      files: {
        tracksInput: path.join(__dirname, "..", "data", "tracks.csv"),
        artistsInput: path.join(__dirname, "..", "data", "artists.csv"),
        tracksOutput: path.join(
          __dirname,
          "..",
          "data",
          "transformedTracks.csv"
        ),
        artistsOutput: path.join(
          __dirname,
          "..",
          "data",
          "transformedArtists.csv"
        ),
      },
      transform: {
        minDuration: 60000,
      },
    });
  });
});
