import amqplib from "amqplib";
import type { ChannelModel } from "amqplib";

export class ConnectionManager {
  private static connection?: ChannelModel;
  private static connectionPromise?: Promise<ChannelModel>;
  private static isShuttingDown = false;

  static async getConnection(
    url: string,
    maxRetries: number,
  ): Promise<ChannelModel> {
    if (this.connection) return this.connection;

    if (!this.connectionPromise) {
      this.connectionPromise = (async () => {
        let retryCount = 0;

        while (!this.isShuttingDown) {
          try {
            const conn = await amqplib.connect(url);

            conn.on("error", () => this.clearConnection());
            conn.on("close", () => this.clearConnection());

            this.connection = conn;
            return conn;
          } catch (err) {
            retryCount++;

            if (maxRetries !== -1 && retryCount >= maxRetries) {
              this.connectionPromise = undefined;
              throw new Error(
                `[RabbitMQ] Connection failed after ${retryCount} attempts.`,
              );
            }

            const delay = Math.min(Math.pow(2, retryCount) * 1000, 30000);
            console.warn(`[RabbitMQ] Retrying connection in ${delay}ms...`);
            await new Promise((res) => setTimeout(res, delay));
          }
        }
        throw new Error("[RabbitMQ] Connection process aborted.");
      })();
    }

    return this.connectionPromise;
  }

  private static clearConnection() {
    this.connection = undefined;
    this.connectionPromise = undefined;
  }

  static async close() {
    this.isShuttingDown = true;
    if (this.connection) {
      await this.connection.close();
    }
  }
}
