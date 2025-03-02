// tests/index.test.ts

import { config } from "../src/config/config";
import { logger } from "../src/utils/logger";

// Mock the transformer module to control behavior of processCSV, FilterTransform, and ArtistFilterTransform.
jest.mock("../src/transformer", () => {
  return {
    processCSV: jest.fn().mockResolvedValue(undefined),
    FilterTransform: jest.fn().mockImplementation((options) => {
      return {
        uniqueArtistIds: new Set(["artist1", "artist2"]),
      };
    }),
    ArtistFilterTransform: jest.fn().mockImplementation((ids) => {
      return {}; // Return a dummy transform instance.
    }),
  };
});

// Import the mocked functions to verify calls.
import {
  processCSV,
  FilterTransform,
  ArtistFilterTransform,
} from "../src/transformer";

// Mock the logger to capture logs.
jest.mock("../src/utils/logger", () => {
  return {
    logger: {
      info: jest.fn(),
      error: jest.fn(),
    },
  };
});

// Import the function to test.
import { runTransformation } from "../src/index";

describe("runTransformation", () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it("should process tracks and artists CSV files", async () => {
    await runTransformation();

    // processCSV should be called twice.
    expect(processCSV).toHaveBeenCalledTimes(2);

    // First call: tracks.
    expect(processCSV).toHaveBeenNthCalledWith(
      1,
      config.files.tracksInput,
      config.files.tracksOutput,
      expect.any(Object) // Instance of FilterTransform
    );

    // Second call: artists.
    expect(processCSV).toHaveBeenNthCalledWith(
      2,
      config.files.artistsInput,
      config.files.artistsOutput,
      expect.any(Object) // Instance of ArtistFilterTransform
    );

    // Verify that logger.info was called with messages for both output files.
    expect(logger.info).toHaveBeenCalledWith(
      `Transformed tracks saved to: ${config.files.tracksOutput}`
    );
    expect(logger.info).toHaveBeenCalledWith(
      `Transformed artists saved to: ${config.files.artistsOutput}`
    );
  });

  it("should log error if processCSV throws an error", async () => {
    // Simulate an error in the first call to processCSV.
    (processCSV as jest.Mock).mockRejectedValueOnce(new Error("Test Error"));

    await runTransformation();

    // Expect that logger.error is called with a message including "Error running transformation:"
    expect(logger.error).toHaveBeenCalledWith(
      expect.stringContaining("Error running transformation:")
    );
  });
});
