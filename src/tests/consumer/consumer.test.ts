// @ts-nocheck
import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import type { ConsumeMessage } from "amqplib";
import { Consumer } from "../../consumer/index.js";
import { ConnectionManager } from "../../connectionManager.js";

jest.mock("../../connectionManager.js", () => ({
  ConnectionManager: {
    getConnection: jest.fn(),
    isConnected: jest.fn(),
  },
}));

const mockGetConnection = ConnectionManager.getConnection as jest.Mock;

class TestConsumer extends Consumer<{ id: number }> {
  async onMessage(data: { id: number }): Promise<void> {
    if (data.id === 999) throw new Error("processing failed");
  }
}

describe("Consumer", () => {
  let channel: any;

  beforeEach(() => {
    channel = {
      prefetch: jest.fn().mockResolvedValue(undefined),
      assertQueue: jest.fn().mockResolvedValue(undefined),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
      unbindQueue: jest.fn().mockResolvedValue(undefined),
      consume: jest.fn(),
      ack: jest.fn(),
      nack: jest.fn(),
      on: jest.fn(),
      once: jest.fn(),
      removeListener: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined),
    };
    mockGetConnection.mockResolvedValue({ createChannel: jest.fn().mockResolvedValue(channel) });
  });

  it("configures DLQ at consumer initialization, not consume()", async () => {
    const consumer = new TestConsumer("amqp://localhost", { useDLQ: true });
    await consumer.consume("orders");

    expect(channel.assertExchange).toHaveBeenCalledWith("orders_dlx", "direct", { durable: true });
    expect(channel.assertQueue).toHaveBeenCalledWith("orders_failed", { durable: true });
    expect(channel.bindQueue).toHaveBeenCalledWith("orders_failed", "orders_dlx", "dead-letter");
    expect(channel.assertQueue).toHaveBeenCalledWith("orders", {
      durable: true,
      deadLetterExchange: "orders_dlx",
      deadLetterRoutingKey: "dead-letter",
    });
  });

  it("does not accept per-consume DLQ configuration", async () => {
    const consumer = new TestConsumer("amqp://localhost");
    await consumer.consume("orders", { prefetch: 5 });

    expect(channel.assertExchange).not.toHaveBeenCalled();
    expect(channel.assertQueue).toHaveBeenCalledWith("orders", { durable: true });
  });

  it("nacks processing failures without requeue when DLQ is enabled", async () => {
    let callback: any;
    channel.consume.mockImplementation((_queue, handler) => { callback = handler; });
    const consumer = new TestConsumer("amqp://localhost", { useDLQ: true });
    await consumer.consume("orders");

    const msg = { content: Buffer.from('{"id":999}'), properties: {} } as ConsumeMessage;
    await callback(msg);

    expect(channel.nack).toHaveBeenCalledWith(msg, false, false);
  });

  it("requeues processing failures when DLQ is disabled", async () => {
    let callback: any;
    channel.consume.mockImplementation((_queue, handler) => { callback = handler; });
    const consumer = new TestConsumer("amqp://localhost");
    await consumer.consume("orders");

    const msg = { content: Buffer.from('{"id":999}'), properties: {} } as ConsumeMessage;
    await callback(msg);

    expect(channel.nack).toHaveBeenCalledWith(msg, false, true);
  });

  it("does not remove listeners owned by other consumers", async () => {
    const consumer = new TestConsumer("amqp://localhost");
    await consumer.consume("orders");

    expect(channel.removeAllListeners).not.toHaveBeenCalled();
  });

  it("restores runtime bindings after a new channel is created", async () => {
    let callback: any;
    channel.consume.mockImplementation((_queue, handler) => { callback = handler; });
    const consumer = new TestConsumer("amqp://localhost");
    await consumer.consume("orders");
    await consumer.bindQueue("orders", "events", "topic", "orders.#");

    const secondChannel = {
      ...channel,
      prefetch: jest.fn().mockResolvedValue(undefined),
      assertQueue: jest.fn().mockResolvedValue(undefined),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
      consume: jest.fn(),
      on: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined),
    };
    mockGetConnection.mockResolvedValue({ createChannel: jest.fn().mockResolvedValue(secondChannel) });
    (consumer as any).channel = undefined;
    await (consumer as any).startConsumption("orders", {}, true);

    expect(secondChannel.bindQueue).toHaveBeenCalledWith("orders", "events", "orders.#");
  });
});
