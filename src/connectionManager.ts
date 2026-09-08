import amqplib from "amqplib";
import type { ChannelModel } from "amqplib";
import { Logger, RabbitConnectionError } from "./types.js";

export type AmqplibModule = typeof import("amqplib");
let _amqplib: AmqplibModule = amqplib;

export class ConnectionManager {
  private static connections = new Map<string, ChannelModel>();
  private static connectionPromises = new Map<string, Promise<ChannelModel>>();
  private static isShuttingDown = false;
  private static logger: Logger = console;

  static setLogger(logger: Logger): void {
    this.logger = logger;
  }

  static __setAmqplib(mock: AmqplibModule): void {
    _amqplib = mock;
  }

  static __resetAmqplib(): void {
    _amqplib = amqplib;
  }

  static async getConnection(url: string, maxRetries: number): Promise<ChannelModel> {
    const existing = this.connections.get(url);
    if (existing) return existing;

    const pending = this.connectionPromises.get(url);
    if (pending) return pending;

    const promise = this.createConnectionWithRetry(url, maxRetries);
    this.connectionPromises.set(url, promise);

    try {
      return await promise;
    } finally {
      if (this.connectionPromises.get(url) === promise) {
        this.connectionPromises.delete(url);
      }
    }
  }

  private static async createConnectionWithRetry(
    url: string,
    maxRetries: number,
  ): Promise<ChannelModel> {
    let retryCount = 0;

    while (!this.isShuttingDown) {
      try {
        const conn = await _amqplib.connect(url);
        conn.on("error", () => this.clearConnection(url, conn));
        conn.on("close", () => this.clearConnection(url, conn));
        this.connections.set(url, conn);
        return conn;
      } catch (err: unknown) {
        retryCount++;
        if (maxRetries !== -1 && retryCount >= maxRetries) {
          throw new RabbitConnectionError(
            `Connection to ${url} failed after ${retryCount} attempts.`,
            err,
          );
        }

        const delay = this.getRetryDelay(retryCount);
        this.logger.warn(
          `[RabbitMQ] Retrying connection in ${delay}ms... (attempt ${retryCount}/${maxRetries === -1 ? "∞" : maxRetries})`,
        );
        await this.sleep(delay);
      }
    }

    throw new RabbitConnectionError("[RabbitMQ] Connection process aborted.");
  }

  private static getRetryDelay(retryCount: number): number {
    return Math.min(Math.pow(2, Math.max(0, retryCount - 1)) * 1000, 30000);
  }

  private static sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private static clearConnection(url: string, connection: ChannelModel): void {
    if (this.connections.get(url) === connection) {
      this.connections.delete(url);
    }
  }

  static async close(url?: string): Promise<void> {
    if (url) {
      const conn = this.connections.get(url);
      if (!conn) return;
      this.connections.delete(url);
      try {
        await conn.close();
      } catch {
        // Ignore close errors.
      }
      return;
    }

    this.isShuttingDown = true;
    try {
      const entries = Array.from(this.connections.entries());
      this.connections.clear();
      await Promise.all(
        entries.map(async ([, conn]) => {
          try {
            await conn.close();
          } catch {
            // Ignore close errors.
          }
        }),
      );
    } finally {
      this.isShuttingDown = false;
    }
  }

  static isConnected(url: string): boolean {
    return this.connections.has(url);
  }
}
