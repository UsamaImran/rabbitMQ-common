// @ts-nocheck

import {
  jest,
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
} from "@jest/globals";

// Mock ConnectionManager
const mockGetConnection = jest.fn();
const mockIsConnected = jest.fn();

jest.mock("../connectionManager.js", () => ({
  ConnectionManager: {
    getConnection: mockGetConnection,
    isConnected: mockIsConnected,
  },
}));

import { Consumer } from "../consumer.js";
import type { ConsumeMessage } from "amqplib";

// Concrete consumer implementation for testing
class TestConsumer extends Consumer<{ id: number; name: string }> {
  public onMessageCalls: Array<{
    data: { id: number; name: string };
    msg: ConsumeMessage;
  }> = [];
  public onMessageShouldThrow = false;

  async onMessage(
    data: { id: number; name: string },
    msg: ConsumeMessage,
  ): Promise<void> {
    this.onMessageCalls.push({ data, msg });
    if (this.onMessageShouldThrow) {
      throw new Error("onMessage error");
    }
  }
}

describe("Consumer", () => {
  const testUrl = "amqp://localhost:5672";
  let mockChannel: any;
  let consumer: TestConsumer;
  let consumeCallback: (msg: ConsumeMessage | null) => void;

  beforeEach(async () => {
    mockChannel = {
      prefetch: jest.fn().mockResolvedValue(undefined),
      assertQueue: jest.fn().mockResolvedValue({ queue: "test-queue" }),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
      unbindQueue: jest.fn().mockResolvedValue(undefined),
      consume: jest
        .fn()
        .mockImplementation((_queue: string, cb: (msg: any) => void) => {
          consumeCallback = cb;
          return { consumerTag: "test-tag" };
        }),
      ack: jest.fn(),
      nack: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
      removeAllListeners: jest.fn(),
    };

    mockGetConnection.mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });

    consumer = new TestConsumer(testUrl);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("consume", () => {
    const queue = "test-queue";

    it("should set prefetch", async () => {
      await consumer.consume(queue, { prefetch: 5 });
      expect(mockChannel.prefetch).toHaveBeenCalledWith(5);
    });

    it("should assert queue with durable true by default", async () => {
      await consumer.consume(queue);
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(queue, {
        durable: true,
      });
    });

    it("should bind to exchange when provided", async () => {
      await consumer.consume(queue, {
        exchange: "test-exchange",
        exchangeType: "topic",
        routingKey: "test.key",
      });

      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-exchange",
        "topic",
        {
          durable: true,
        },
      );
      expect(mockChannel.bindQueue).toHaveBeenCalledWith(
        queue,
        "test-exchange",
        "test.key",
      );
    });
  });

  describe("message handling", () => {
    const queue = "test-queue";
    const mockMessage = {
      content: Buffer.from(JSON.stringify({ id: 1, name: "test" })),
      properties: { correlationId: "corr-123" },
      fields: { routingKey: "test" },
    } as ConsumeMessage;

    beforeEach(async () => {
      await consumer.consume(queue);
    });

    it("should parse JSON and call onMessage", async () => {
      consumeCallback(mockMessage);
      await new Promise(process.nextTick);

      expect(consumer.onMessageCalls).toHaveLength(1);
      expect(consumer.onMessageCalls[0].data).toEqual({ id: 1, name: "test" });
      expect(mockChannel.ack).toHaveBeenCalledWith(mockMessage);
    });

    it("should handle JSON parse errors", async () => {
      const invalidMessage = {
        ...mockMessage,
        content: Buffer.from("invalid json"),
      } as ConsumeMessage;

      consumeCallback(invalidMessage);
      await new Promise(process.nextTick);

      expect(mockChannel.nack).toHaveBeenCalledWith(
        invalidMessage,
        false,
        false,
      );
    });

    it("should handle onMessage throwing error", async () => {
      consumer.onMessageShouldThrow = true;
      consumeCallback(mockMessage);
      await new Promise(process.nextTick);

      expect(mockChannel.nack).toHaveBeenCalledWith(mockMessage, false, true);
    });
  });

  describe("getCurrentQueue", () => {
    it("should return undefined before consume", () => {
      expect(consumer.getCurrentQueue()).toBeUndefined();
    });

    it("should return queue after consume", async () => {
      await consumer.consume("test-queue");
      expect(consumer.getCurrentQueue()).toBe("test-queue");
    });
  });
});
