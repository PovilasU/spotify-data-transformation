// tests/config.test.ts
import path from "path";

describe("Configuration", () => {
  const ORIGINAL_ENV = process.env;

  beforeEach(() => {
    // Create a clean copy of process.env.
    process.env = { ...ORIGINAL_ENV };
    // Remove any preloaded values that might come from a .env file.
    delete process.env.AWS_ACCESS_KEY_ID;
    delete process.env.AWS_SECRET_ACCESS_KEY;
    delete process.env.AWS_REGION;
    delete process.env.S3_BUCKET_NAME;
    delete process.env.PG_HOST;
    delete process.env.PG_PORT;
    delete process.env.PG_USER;
    delete process.env.PG_PASSWORD;
    delete process.env.PG_DATABASE;
    delete process.env.LOCAL_TEST;
    // Clear module cache so that changes to process.env are picked up.
    jest.resetModules();
  });

  afterEach(() => {
    process.env = ORIGINAL_ENV;
    jest.resetModules();
    jest.dontMock("dotenv");
  });

  it("should load config correctly when all required env variables are provided", () => {
    // Set required environment variables.
    process.env.AWS_ACCESS_KEY_ID = "test_access_key";
    process.env.AWS_SECRET_ACCESS_KEY = "test_secret_key";
    process.env.AWS_REGION = "test_region";
    process.env.S3_BUCKET_NAME = "test_bucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "test_user";
    process.env.PG_PASSWORD = "test_password";
    process.env.PG_DATABASE = "test_db";
    process.env.LOCAL_TEST = "false"; // This means do NOT load local files

    // Reset modules so that the config module reads the updated env variables.
    jest.resetModules();
    const { config } = require("../src/config/config");

    // Check AWS config values.
    expect(config.aws.accessKeyId).toBe("test_access_key");
    expect(config.aws.secretAccessKey).toBe("test_secret_key");
    expect(config.aws.region).toBe("test_region");
    expect(config.aws.bucketName).toBe("test_bucket");

    // Check PostgreSQL config.
    expect(config.postgres.host).toBe("localhost");
    expect(config.postgres.port).toBe(5432);
    expect(config.postgres.user).toBe("test_user");
    expect(config.postgres.password).toBe("test_password");
    expect(config.postgres.database).toBe("test_db");

    // Check s3Keys.
    expect(config.s3Keys.tracks).toBe("transformedTracks.csv");
    expect(config.s3Keys.artists).toBe("transformedArtists.csv");

    // Verify that the file paths are as defined.
    // The config file constructs paths using:
    //   path.join(__dirname, "..", "..", "data", "<file>")
    // where __dirname in config is something like:
    //   .../spotify-data-transformation/src/config
    // so the resolved path will be:
    //   .../spotify-data-transformation/data/<file>
    // In this test, __dirname is the tests folder (e.g., .../spotify-data-transformation/tests),
    // so we build the expected paths relative to the project root.
    const expectedTracksInput = path.join(
      __dirname,
      "..",
      "data",
      "tracks.csv"
    );
    const expectedTracksOutput = path.join(
      __dirname,
      "..",
      "data",
      "transformedTracks.csv"
    );
    expect(config.files.tracksInput).toBe(expectedTracksInput);
    expect(config.files.tracksOutput).toBe(expectedTracksOutput);

    // Based on your config, localTest is true when process.env.LOCAL_TEST is exactly "false".
    expect(config.localTest).toBe(true);
  });

  it("should set localTest to false when LOCAL_TEST is not 'false'", () => {
    // Set required environment variables.
    process.env.AWS_ACCESS_KEY_ID = "test_access_key";
    process.env.AWS_SECRET_ACCESS_KEY = "test_secret_key";
    process.env.AWS_REGION = "test_region";
    process.env.S3_BUCKET_NAME = "test_bucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "test_user";
    process.env.PG_PASSWORD = "test_password";
    process.env.PG_DATABASE = "test_db";
    process.env.LOCAL_TEST = "true"; // Any value other than "false" should result in false

    jest.resetModules();
    const { config } = require("../src/config/config");
    expect(config.localTest).toBe(false);
  });

  it("should throw an error if a required environment variable is missing", () => {
    // For this test we want to simulate that no .env values are loaded.
    // We override dotenv.config to return an empty object.
    jest.doMock("dotenv", () => ({
      config: jest.fn(() => ({ parsed: {} })),
    }));
    // Set all required variables except AWS_ACCESS_KEY_ID.
    process.env.AWS_SECRET_ACCESS_KEY = "test_secret_key";
    process.env.AWS_REGION = "test_region";
    process.env.S3_BUCKET_NAME = "test_bucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "test_user";
    process.env.PG_PASSWORD = "test_password";
    process.env.PG_DATABASE = "test_db";
    process.env.LOCAL_TEST = "false";

    jest.resetModules();

    expect(() => {
      require("../src/config/config");
    }).toThrowError(/Missing required environment variable: AWS_ACCESS_KEY_ID/);
  });
});
