// tests/logger.test.ts
import path from "path";
import { logger } from "../src/utils/logger";
import { transports } from "winston";

describe("Logger configuration", () => {
  it("should have level 'info'", () => {
    expect(logger.level).toBe("info");
  });

  it("should include a Console transport", () => {
    const consoleTransport = logger.transports.find(
      (transport) => transport instanceof transports.Console
    );
    expect(consoleTransport).toBeDefined();
  });

  it("should include a File transport with the correct basename", () => {
    const fileTransport = logger.transports.find(
      (transport) => transport instanceof transports.File
    );
    expect(fileTransport).toBeDefined();

    // Compute the expected basename
    const currentDate = new Date().toISOString().slice(0, 10);
    const expectedBasename = `app-error-${currentDate}.log`;

    // Get the actual filename from the file transport.
    // Depending on the version of Winston, it might be relative.
    const actualFilename = (fileTransport as transports.FileTransportInstance)
      .filename;
    expect(path.basename(actualFilename)).toBe(expectedBasename);
  });
});
