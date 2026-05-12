import amqplib from "amqplib";
import type { ChannelModel } from "amqplib";

export class ConnectionManager {
  private static connection?: ChannelModel;
  private static connectionPromise?: Promise<ChannelModel>;

  static async getConnection(url: string): Promise<ChannelModel> {
    if (this.connection) return this.connection;

    if (!this.connectionPromise) {
      this.connectionPromise = amqplib.connect(url).then((conn) => {
        this.connection = conn;

        conn.on("error", (err) => {
          console.error("[RabbitMQ] Connection error:", err);
          this.connection = undefined;
          this.connectionPromise = undefined;
        });

        conn.on("close", () => {
          console.warn("[RabbitMQ] Connection closed");
          this.connection = undefined;
          this.connectionPromise = undefined;
        });

        return conn;
      });
    }

    return this.connectionPromise;
  }
}
