// @ts-nocheck
import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { Producer } from "../../producer/index.js";
import { RabbitPublishError } from "../../types.js";
import { ConnectionManager } from "../../connectionManager.js";

jest.mock("../../connectionManager.js", () => ({
  ConnectionManager: { getConnection: jest.fn(), isConnected: jest.fn() },
}));

const mockGetConnection = ConnectionManager.getConnection as jest.Mock;

describe("Producer", () => {
  let channel: any;

  beforeEach(() => {
    channel = {
      assertQueue: jest.fn().mockResolvedValue(undefined),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      sendToQueue: jest.fn().mockReturnValue(true),
      publish: jest.fn().mockReturnValue(true),
      on: jest.fn(),
      once: jest.fn(),
      removeListener: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined),
    };
    mockGetConnection.mockResolvedValue({ createChannel: jest.fn().mockResolvedValue(channel) });
  });

  it("publishes to a queue", async () => {
    const producer = new Producer("amqp://localhost");
    expect(await producer.publish("orders", { id: 1 })).toBe(true);
    expect(channel.assertQueue).toHaveBeenCalledWith("orders", { durable: true });
    expect(channel.sendToQueue).toHaveBeenCalledTimes(1);
  });

  it("configures DLQ at producer initialization", async () => {
    const producer = new Producer("amqp://localhost", { useDLQ: true });
    await producer.publish("orders", { id: 1 });

    expect(channel.assertExchange).toHaveBeenCalledWith("orders_dlx", "direct", { durable: true });
    expect(channel.assertQueue).toHaveBeenCalledWith("orders_failed", { durable: true });
    expect(channel.assertQueue).toHaveBeenCalledWith("orders", {
      durable: true,
      deadLetterExchange: "orders_dlx",
      deadLetterRoutingKey: "dead-letter",
    });
  });

  it("does not duplicate a queue assertion with identical topology", async () => {
    const producer = new Producer("amqp://localhost");
    await producer.publish("orders", { id: 1 });
    await producer.publish("orders", { id: 2 });
    expect(channel.assertQueue).toHaveBeenCalledTimes(1);
  });

  it("reasserts topology after a channel replacement", async () => {
    const producer = new Producer("amqp://localhost");
    await producer.publish("orders", { id: 1 });
    (producer as any).channel = undefined;
    await producer.publish("orders", { id: 2 });
    expect(channel.assertQueue).toHaveBeenCalledTimes(2);
  });

  it("throws RabbitPublishError for topology failures", async () => {
    channel.assertQueue.mockRejectedValue(new Error("PRECONDITION_FAILED"));
    const producer = new Producer("amqp://localhost");
    await expect(producer.publish("orders", { id: 1 })).rejects.toBeInstanceOf(RabbitPublishError);
  });

  it("waits for drain without retrying the message", async () => {
    const producer = new Producer("amqp://localhost");
    const wait = jest.spyOn(producer, "waitForDrain").mockResolvedValue(undefined);
    channel.sendToQueue.mockReturnValueOnce(false).mockReturnValueOnce(true);

    await producer.publishBatch("orders", [{ id: 1 }, { id: 2 }]);

    expect(wait).toHaveBeenCalledTimes(1);
    expect(channel.sendToQueue).toHaveBeenCalledTimes(2);
  });
});
