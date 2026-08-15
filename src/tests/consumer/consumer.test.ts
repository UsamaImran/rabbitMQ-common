// @ts-nocheck
import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  jest,
} from "@jest/globals";
import type { ConsumeMessage } from "amqplib";
import { Consumer } from "../../consumer/index.js";

// Mock ConnectionManager
const mockGetConnection = jest.fn();
const mockIsConnected = jest.fn();

jest.mock("../../src/base/connectionManager.js", () => ({
  ConnectionManager: {
    getConnection: mockGetConnection,
    isConnected: mockIsConnected,
  },
}));

// Test consumer implementation
class TestConsumer extends Consumer<{ id: number; name: string }> {
  public processedMessages: Array<{ id: number; name: string }> = [];
  public errors: Error[] = [];

  async onMessage(
    data: { id: number; name: string },
    msg: ConsumeMessage,
  ): Promise<void> {
    this.processedMessages.push(data);
    // Simulate error for testing
    if (data.id === 999) {
      throw new Error("Test error");
    }
  }

  async onError(
    error: Error,
    data?: { id: number; name: string },
  ): Promise<void> {
    this.errors.push(error);
  }
}

describe("Consumer Integration", () => {
  const testUrl = "amqp://localhost:5672";
  let mockChannel: any;
  let consumer: TestConsumer;
  let consumeCallback: any;

  beforeEach(async () => {
    mockChannel = {
      prefetch: jest.fn().mockResolvedValue(undefined),
      assertQueue: jest.fn().mockResolvedValue(undefined),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
      unbindQueue: jest.fn().mockResolvedValue(undefined),
      consume: jest.fn().mockImplementation((queue, callback) => {
        consumeCallback = callback;
      }),
      ack: jest.fn(),
      nack: jest.fn(),
      on: jest.fn(),
      once: jest.fn(),
      removeAllListeners: jest.fn(),
      close: jest.fn().mockResolvedValue(undefined),
    } as any;

    mockGetConnection.mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });

    consumer = new TestConsumer(testUrl);
    await consumer["getChannel"]();
  });

  afterEach(() => {
    jest.clearAllMocks();
    jest.restoreAllMocks();
  });

  describe("consume", () => {
    it("should start consuming from queue", async () => {
      await consumer.consume("test-queue");

      expect(mockChannel.prefetch).toHaveBeenCalledWith(1);
      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: true,
      });
      expect(mockChannel.consume).toHaveBeenCalledWith(
        "test-queue",
        expect.any(Function),
      );
      expect(consumer.isActive()).toBe(true);
    });

    it("should setup DLQ when enabled", async () => {
      await consumer.consume("test-queue", { useDLQ: true });

      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-queue_dlx",
        "direct",
        { durable: true },
      );
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(
        "test-queue_failed",
        {
          durable: true,
        },
      );
      expect(mockChannel.bindQueue).toHaveBeenCalledWith(
        "test-queue_failed",
        "test-queue_dlx",
        "dead-letter",
      );
    });

    it("should bind to exchange when provided", async () => {
      await consumer.consume("test-queue", {
        exchange: "test-exchange",
        exchangeType: "topic",
        routingKey: "test.key",
      });

      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-exchange",
        "topic",
        { durable: true },
      );
      expect(mockChannel.bindQueue).toHaveBeenCalledWith(
        "test-queue",
        "test-exchange",
        "test.key",
      );
    });

    it("should set up recovery listeners", async () => {
      await consumer.consume("test-queue");

      expect(mockChannel.removeAllListeners).toHaveBeenCalledWith("close");
      expect(mockChannel.removeAllListeners).toHaveBeenCalledWith("error");
      expect(mockChannel.on).toHaveBeenCalledWith(
        "close",
        expect.any(Function),
      );
      expect(mockChannel.on).toHaveBeenCalledWith(
        "error",
        expect.any(Function),
      );
    });

    it("should handle consumption errors with recovery", async () => {
      // Make consume fail
      mockChannel.consume.mockRejectedValue(new Error("Consume failed"));

      // Spy on handleRecovery
      const recoverSpy = jest.spyOn(consumer as any, "handleRecovery");

      await consumer.consume("test-queue");

      expect(recoverSpy).toHaveBeenCalled();
    });
  });

  describe("message processing", () => {
    it("should process valid messages", async () => {
      await consumer.consume("test-queue");

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1, name: "test" })),
        properties: { correlationId: "corr-123" },
      };

      await consumeCallback(testMessage);

      expect(consumer.processedMessages).toHaveLength(1);
      expect(consumer.processedMessages[0]).toEqual({ id: 1, name: "test" });
      expect(mockChannel.ack).toHaveBeenCalledWith(testMessage);
    });

    it("should handle malformed JSON", async () => {
      await consumer.consume("test-queue", { useDLQ: true });

      const testMessage = {
        content: Buffer.from("invalid json"),
        properties: {},
      };

      await consumeCallback(testMessage);

      expect(consumer.errors).toHaveLength(1);
      expect(consumer.errors[0].message).toContain("Failed to parse message");
      expect(mockChannel.nack).toHaveBeenCalledWith(testMessage, false, false);
    });

    it("should handle user errors with requeue when DLQ disabled", async () => {
      await consumer.consume("test-queue", { useDLQ: false });

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 999, name: "error" })),
        properties: {},
      };

      await consumeCallback(testMessage);

      expect(consumer.errors).toHaveLength(1);
      expect(consumer.errors[0].message).toBe("Test error");
      expect(mockChannel.nack).toHaveBeenCalledWith(testMessage, false, true);
    });

    it("should handle user errors without requeue when DLQ enabled", async () => {
      await consumer.consume("test-queue", { useDLQ: true });

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 999, name: "error" })),
        properties: {},
      };

      await consumeCallback(testMessage);

      expect(consumer.errors).toHaveLength(1);
      expect(mockChannel.nack).toHaveBeenCalledWith(testMessage, false, false);
    });
  });

  describe("bindQueue", () => {
    it("should bind queue at runtime", async () => {
      await consumer.consume("test-queue");

      await consumer.bindQueue(
        "test-queue",
        "new-exchange",
        "topic",
        "new.key",
      );

      expect(mockChannel.bindQueue).toHaveBeenCalledWith(
        "test-queue",
        "new-exchange",
        "new.key",
      );
    });

    it("should throw error when not consuming from queue", async () => {
      await expect(
        consumer.bindQueue("wrong-queue", "exchange", "topic"),
      ).rejects.toThrow(
        'Cannot bind: not currently consuming from queue "wrong-queue"',
      );
    });

    it("should throw error when not consuming at all", async () => {
      const newConsumer = new TestConsumer(testUrl);

      await expect(
        newConsumer.bindQueue("test-queue", "exchange", "topic"),
      ).rejects.toThrow(
        'Cannot bind: not currently consuming from queue "test-queue"',
      );
    });
  });

  describe("unbindQueue", () => {
    it("should unbind queue at runtime", async () => {
      await consumer.consume("test-queue");

      await consumer.unbindQueue("test-queue", "test-exchange", "test.key");

      expect(mockChannel.unbindQueue).toHaveBeenCalledWith(
        "test-queue",
        "test-exchange",
        "test.key",
      );
    });
  });

  describe("getCurrentQueue", () => {
    it("should return undefined when not consuming", () => {
      expect(consumer.getCurrentQueue()).toBeUndefined();
    });

    it("should return current queue when consuming", async () => {
      await consumer.consume("test-queue");
      expect(consumer.getCurrentQueue()).toBe("test-queue");
    });
  });

  describe("forceRecover", () => {
    it("should trigger recovery when consuming", async () => {
      await consumer.consume("test-queue");

      const recoverSpy = jest.spyOn(consumer as any, "handleRecovery");
      await consumer.forceRecover();

      expect(recoverSpy).toHaveBeenCalled();
    });

    it("should not trigger recovery when not consuming", async () => {
      const recoverSpy = jest.spyOn(consumer as any, "handleRecovery");
      await consumer.forceRecover();

      expect(recoverSpy).not.toHaveBeenCalled();
    });
  });

  describe("close", () => {
    it("should close consumer and cleanup", async () => {
      await consumer.consume("test-queue");
      await consumer.close();

      expect(mockChannel.close).toHaveBeenCalled();
      expect(consumer.isActive()).toBe(false);
      expect(consumer.getActiveBindings()).toHaveLength(0);
    });
  });

  describe("getActiveBindings", () => {
    it("should return active bindings", async () => {
      await consumer.consume("test-queue", {
        exchange: "test-exchange",
        exchangeType: "topic",
        routingKey: "test.key",
      });

      const bindings = consumer.getActiveBindings();
      expect(bindings).toContain("test-queue:test-exchange:test.key");
    });
  });

  describe("isActive", () => {
    it("should return false before consuming", () => {
      expect(consumer.isActive()).toBe(false);
    });

    it("should return true after consuming", async () => {
      await consumer.consume("test-queue");
      expect(consumer.isActive()).toBe(true);
    });

    it("should return false after close", async () => {
      await consumer.consume("test-queue");
      await consumer.close();
      expect(consumer.isActive()).toBe(false);
    });
  });
});
