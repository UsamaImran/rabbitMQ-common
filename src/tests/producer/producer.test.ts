// @ts-nocheck
import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  jest,
} from "@jest/globals";
import { Producer } from "../../producer/index.js";
import { RabbitPublishError } from "../../types.js";

// Mock ConnectionManager
const mockGetConnection = jest.fn();
const mockIsConnected = jest.fn();

jest.mock("../../base/ConnectionManager.js", () => ({
  ConnectionManager: {
    getConnection: mockGetConnection,
    isConnected: mockIsConnected,
  },
}));

describe("Producer (Integration)", () => {
  const testUrl = "amqp://localhost:5672";
  let mockChannel: any;
  let producer: Producer;

  beforeEach(async () => {
    mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: "test-queue" }),
      sendToQueue: jest.fn().mockReturnValue(true),
      publish: jest.fn().mockReturnValue(true),
      once: jest.fn(),
      on: jest.fn(),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      close: jest.fn().mockResolvedValue(undefined),
    };

    mockGetConnection.mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });

    producer = new Producer(testUrl);
    await producer["getChannel"]();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("publish", () => {
    it("should publish a single message", async () => {
      const result = await producer.publish("test-queue", { id: 1 });

      expect(result).toBe(true);
      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(1);
    });

    it("should assert queue on first publish", async () => {
      await producer.publish("test-queue", { id: 1 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should not assert queue on subsequent publishes", async () => {
      await producer.publish("test-queue", { id: 1 });
      await producer.publish("test-queue", { id: 2 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should throw RabbitPublishError on failure", async () => {
      mockChannel.sendToQueue.mockImplementation(() => {
        throw new Error("Channel closed");
      });

      await expect(producer.publish("test-queue", { id: 1 })).rejects.toThrow(
        RabbitPublishError,
      );
    });
  });

  describe("publishBatch", () => {
    const messages = [
      { id: 1, name: "test1" },
      { id: 2, name: "test2" },
      { id: 3, name: "test3" },
    ];

    it("should publish all messages in batch", async () => {
      const result = await producer.publishBatch("test-queue", messages);

      expect(result.total).toBe(3);
      expect(result.successful).toBe(3);
      expect(result.failed).toBe(0);
      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(3);
    });

    it("should assert queue once for batch", async () => {
      await producer.publishBatch("test-queue", messages);

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should handle failures in batch", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(true)
        .mockImplementationOnce(() => {
          throw new Error("Failed");
        })
        .mockReturnValueOnce(true);

      const result = await producer.publishBatch("test-queue", messages);

      expect(result.successful).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors).toHaveLength(1);
    });

    it("should return empty result for empty batch", async () => {
      const result = await producer.publishBatch("test-queue", []);

      expect(result).toEqual({
        total: 0,
        successful: 0,
        failed: 0,
        errors: [],
      });
      expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
    });
  });

  describe("publishToExchange", () => {
    it("should publish to exchange", async () => {
      const result = await producer.publishToExchange(
        "test-exchange",
        "fanout",
        { id: 1 },
      );

      expect(result).toBe(true);
      expect(mockChannel.publish).toHaveBeenCalledTimes(1);
    });

    it("should assert exchange on first publish", async () => {
      await producer.publishToExchange("test-exchange", "fanout", { id: 1 });

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(1);
    });
  });

  describe("publishBatchToExchange", () => {
    const messages = [
      { id: 1, name: "test1" },
      { id: 2, name: "test2" },
      { id: 3, name: "test3" },
    ];

    it("should publish batch to exchange", async () => {
      const result = await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        messages,
      );

      expect(result.total).toBe(3);
      expect(result.successful).toBe(3);
      expect(mockChannel.publish).toHaveBeenCalledTimes(3);
    });

    it("should assert exchange once for batch", async () => {
      await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        messages,
      );

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(1);
    });
  });

  describe("resetQueueCache", () => {
    it("should reset specific queue cache", async () => {
      await producer.publish("queue-1", { id: 1 });
      await producer.publish("queue-2", { id: 1 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(2);

      producer.resetQueueCache("queue-1");
      await producer.publish("queue-1", { id: 2 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(3);
    });

    it("should reset all queue cache when called without args", async () => {
      await producer.publish("queue-1", { id: 1 });
      await producer.publish("queue-2", { id: 1 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(2);

      producer.resetQueueCache();
      await producer.publish("queue-1", { id: 2 });
      await producer.publish("queue-2", { id: 2 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(4);
    });
  });

  describe("resetExchangeCache", () => {
    it("should reset specific exchange cache", async () => {
      await producer.publishToExchange("exchange-1", "fanout", { id: 1 });
      await producer.publishToExchange("exchange-2", "fanout", { id: 1 });

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(2);

      producer.resetExchangeCache("exchange-1", "fanout");
      await producer.publishToExchange("exchange-1", "fanout", { id: 2 });

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(3);
    });

    it("should reset all exchange cache when called without args", async () => {
      await producer.publishToExchange("exchange-1", "fanout", { id: 1 });
      await producer.publishToExchange("exchange-2", "fanout", { id: 1 });

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(2);

      producer.resetExchangeCache();
      await producer.publishToExchange("exchange-1", "fanout", { id: 2 });
      await producer.publishToExchange("exchange-2", "fanout", { id: 2 });

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(4);
    });
  });

  describe("waitForDrain", () => {
    it("should resolve when drain event fires", async () => {
      mockChannel.once.mockImplementation((event: string, cb: () => void) => {
        if (event === "drain") {
          cb();
        }
      });

      await expect(producer.waitForDrain()).resolves.toBeUndefined();
    });
  });

  describe("close", () => {
    it("should close the channel", async () => {
      await producer.close();
      expect(mockChannel.close).toHaveBeenCalled();
    });
  });

  describe("isConnected", () => {
    it("should check connection status", () => {
      producer.isConnected();
      expect(mockIsConnected).toHaveBeenCalledWith(testUrl);
    });
  });
});
