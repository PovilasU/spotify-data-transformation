import path from "path";
import { createLogger, format, transports } from "winston";

const currentDate = new Date().toISOString().slice(0, 10);

export const logger = createLogger({
  level: "info",
  format: format.combine(
    format.timestamp(),
    format.printf(
      ({ timestamp, level, message }) => `${timestamp} [${level}] ${message}`
    )
  ),
  transports: [
    new transports.Console(),
    new transports.File({
      filename: path.join(
        __dirname,
        "..",
        "..",
        "logs",
        `app-error-${currentDate}.log`
      ),
    }),
  ],
});
