import amqplib from "amqplib";
import type { ChannelModel } from "amqplib";
import { Logger } from "./types.js";

export class ConnectionManager {
  // FIX #1: Keyed by URL so multiple URLs each get their own connection
  private static connections = new Map<string, ChannelModel>();
  private static connectionPromises = new Map<string, Promise<ChannelModel>>();
  private static isShuttingDown = false;
  private static logger: Logger = console;

  static setLogger(logger: Logger): void {
    this.logger = logger;
  }

  static async getConnection(
    url: string,
    maxRetries: number,
  ): Promise<ChannelModel> {
    const existing = this.connections.get(url);
    if (existing) return existing;

    const pending = this.connectionPromises.get(url);
    if (pending) return pending;

    const promise = this.createConnectionWithRetry(url, maxRetries);
    this.connectionPromises.set(url, promise);
    return promise;
  }

  private static async createConnectionWithRetry(
    url: string,
    maxRetries: number,
  ): Promise<ChannelModel> {
    let retryCount = 0;

    while (!this.isShuttingDown) {
      try {
        const conn = await amqplib.connect(url);

        conn.on("error", () => this.clearConnection(url));
        conn.on("close", () => this.clearConnection(url));

        this.connections.set(url, conn);
        return conn;
      } catch (err) {
        retryCount++;

        if (maxRetries !== -1 && retryCount >= maxRetries) {
          this.connectionPromises.delete(url);
          throw new Error(
            `[RabbitMQ] Connection to ${url} failed after ${retryCount} attempts.`,
          );
        }

        const delay = this.getRetryDelay(retryCount);
        this.logger.warn(
          `[RabbitMQ] Retrying connection in ${delay}ms... (attempt ${retryCount}/${maxRetries === -1 ? "∞" : maxRetries})`,
        );
        await this.sleep(delay);
      }
    }

    throw new Error("[RabbitMQ] Connection process aborted.");
  }

  private static getRetryDelay(retryCount: number): number {
    return Math.min(Math.pow(2, retryCount) * 1000, 30000);
  }

  private static sleep(ms: number): Promise<void> {
    return new Promise((res) => setTimeout(res, ms));
  }

  private static clearConnection(url: string): void {
    this.connections.delete(url);
    this.connectionPromises.delete(url);
  }

  // FIX #4: Reset isShuttingDown so the manager can be reused after close()
  static async close(url?: string): Promise<void> {
    if (url) {
      // Close a specific connection
      const conn = this.connections.get(url);
      if (conn) {
        await conn.close();
        this.clearConnection(url);
      }
    } else {
      // Close all connections
      this.isShuttingDown = true;
      const closePromises = Array.from(this.connections.entries()).map(
        async ([u, conn]) => {
          try {
            await conn.close();
          } catch {
            // ignore close errors
          }
          this.clearConnection(u);
        },
      );
      await Promise.all(closePromises);
      // FIX #4: Allow reconnection after close
      this.isShuttingDown = false;
    }
  }

  // Health check utility
  static isConnected(url: string): boolean {
    return this.connections.has(url);
  }
}
