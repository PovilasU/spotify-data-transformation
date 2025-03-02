// tests/config.test.ts
import path from "path";

describe("config module", () => {
  // Save the original environment variables.
  const originalEnv = { ...process.env };

  beforeEach(() => {
    // Reset modules so that changes to process.env are picked up when the module is imported.
    jest.resetModules();
    process.env = { ...originalEnv };
  });

  afterAll(() => {
    // Restore the original environment.
    process.env = originalEnv;
  });

  it("should export config with correct values when all required environment variables are present", () => {
    // Set required environment variables.
    process.env.AWS_ACCESS_KEY_ID = "dummyKey";
    process.env.AWS_SECRET_ACCESS_KEY = "dummySecret";
    process.env.AWS_REGION = "us-east-1";
    process.env.S3_BUCKET_NAME = "dummy-bucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "dummyUser";
    process.env.PG_PASSWORD = "dummyPass";
    process.env.PG_DATABASE = "dummyDB";
    // Optionally, set LOCAL_TEST.
    process.env.LOCAL_TEST = "false";

    // Import the config module after setting process.env.
    const { config } = require("../src/config/config");

    // Verify AWS config.
    expect(config.aws.accessKeyId).toBe("dummyKey");
    expect(config.aws.secretAccessKey).toBe("dummySecret");
    expect(config.aws.region).toBe("us-east-1");
    expect(config.aws.bucketName).toBe("dummy-bucket");

    // Verify PostgreSQL config.
    expect(config.postgres.host).toBe("localhost");
    expect(config.postgres.port).toBe(5432);
    expect(config.postgres.user).toBe("dummyUser");
    expect(config.postgres.password).toBe("dummyPass");
    expect(config.postgres.database).toBe("dummyDB");

    // Verify other config values.
    expect(config.s3Keys.tracks).toBe("transformedTracks.csv");
    expect(config.files.tracksInput).toContain(path.join("data", "tracks.csv"));
    expect(config.transform.minDuration).toBe(60000);
    expect(config.localTest).toBe(false);
  });

  it("should throw an error if a required environment variable is missing", () => {
    // Set all required env vars except PG_DATABASE.
    process.env.AWS_ACCESS_KEY_ID = "dummyKey";
    process.env.AWS_SECRET_ACCESS_KEY = "dummySecret";
    process.env.AWS_REGION = "us-east-1";
    process.env.S3_BUCKET_NAME = "dummy-bucket";
    process.env.PG_HOST = "localhost";
    process.env.PG_PORT = "5432";
    process.env.PG_USER = "dummyUser";
    process.env.PG_PASSWORD = "dummyPass";
    // Instead of deleting, set PG_DATABASE to an empty string.
    process.env.PG_DATABASE = "";

    expect(() => {
      require("../src/config/config");
    }).toThrow("Missing required environment variable: PG_DATABASE");
  });
});
