import winston from "winston";

const logLevel = process.env.LOG_LEVEL || "info";
const logFormat = winston.format.printf(
  (info) => `${info.timestamp} ${info.level} [${info.label}]: ${info.message}`
);

const logger = winston.createLogger({
  level: logLevel,
  format: winston.format.combine(
    winston.format.timestamp({ format: "YYYY-MM-DD HH:mm:ss" }),
    winston.format.errors({ stack: true }),
    logFormat
  ),
  defaultMeta: { service: "trino-proxy" },
});

logger.add(
  new winston.transports.Console({
    format: winston.format.simple(),
  })
);

export default logger;
