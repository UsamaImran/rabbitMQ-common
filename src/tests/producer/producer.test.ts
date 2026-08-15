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

// ✅ Use jest.fn() directly inside the mock
jest.mock("../../connectionManager", () => ({
  ConnectionManager: {
    getConnection: jest.fn(),
    isConnected: jest.fn(),
  },
}));

// Import after mock to get the mocked functions
import { ConnectionManager } from "../../connectionManager.js";

// Get references to the mocked functions
const mockGetConnection = ConnectionManager.getConnection as jest.Mock;
const mockIsConnected = ConnectionManager.isConnected as jest.Mock;

describe("Producer", () => {
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
      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: true,
      });
    });

    it("should not assert queue on subsequent publishes", async () => {
      await producer.publish("test-queue", { id: 1 });
      await producer.publish("test-queue", { id: 2 });

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should apply publish options", async () => {
      await producer.publish(
        "test-queue",
        { id: 1 },
        { persistent: false, expiration: "60000", priority: 5 },
      );

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        "test-queue",
        expect.any(Buffer),
        {
          persistent: false,
          expiration: "60000",
          priority: 5,
        },
      );
    });

    it("should apply queue options", async () => {
      await producer.publish(
        "test-queue",
        { id: 1 },
        {},
        { durable: false, maxLength: 100, messageTtl: 5000, priority: 10 },
      );

      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: false,
        maxLength: 100,
        messageTtl: 5000,
        maxPriority: 10,
      });
    });

    it("should throw RabbitPublishError on failure", async () => {
      mockChannel.sendToQueue.mockImplementation(() => {
        throw new Error("Channel closed");
      });

      await expect(producer.publish("test-queue", { id: 1 })).rejects.toThrow(
        RabbitPublishError,
      );
    });

    it("should return false when buffer is full", async () => {
      mockChannel.sendToQueue.mockReturnValue(false);
      const result = await producer.publish("test-queue", { id: 1 });

      expect(result).toBe(false);
    });

    it("should serialize message as JSON", async () => {
      const message = { id: 1, name: "test", nested: { value: 42 } };
      await producer.publish("test-queue", message);

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        "test-queue",
        Buffer.from(JSON.stringify(message)),
        expect.any(Object),
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

    it("should apply publish options to all messages in batch", async () => {
      await producer.publishBatch("test-queue", messages, {
        persistent: false,
        expiration: "60000",
        priority: 5,
      });

      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(3);
      for (let i = 0; i < messages.length; i++) {
        expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
          "test-queue",
          expect.any(Buffer),
          {
            persistent: false,
            expiration: "60000",
            priority: 5,
          },
        );
      }
    });

    it("should apply queue options once for batch", async () => {
      await producer.publishBatch(
        "test-queue",
        messages,
        {},
        { durable: false, maxLength: 50 },
      );

      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: false,
        maxLength: 50,
      });
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
      expect(result.errors[0].index).toBe(1);
      expect(result.errors[0].message).toEqual(messages[1]);
    });

    it("should handle multiple failures in batch", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(true)
        .mockImplementationOnce(() => {
          throw new Error("Failed 1");
        })
        .mockImplementationOnce(() => {
          throw new Error("Failed 2");
        });

      const result = await producer.publishBatch("test-queue", messages);

      expect(result.successful).toBe(1);
      expect(result.failed).toBe(2);
      expect(result.errors).toHaveLength(2);
      expect(result.errors[0].index).toBe(1);
      expect(result.errors[1].index).toBe(2);
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

    it("should wait for drain when buffer is full", async () => {
      const waitForDrainSpy = jest.spyOn(producer, "waitForDrain");
      waitForDrainSpy.mockResolvedValue(undefined);

      mockChannel.sendToQueue
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      await producer.publishBatch("test-queue", messages);

      expect(waitForDrainSpy).toHaveBeenCalledTimes(1);
    });

    it("should retry after drain when buffer is full", async () => {
      const waitForDrainSpy = jest.spyOn(producer, "waitForDrain");
      waitForDrainSpy.mockResolvedValue(undefined);

      mockChannel.sendToQueue
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      await producer.publishBatch("test-queue", messages);

      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(4);
    });

    it("should mark message as failed if retry still fails", async () => {
      const waitForDrainSpy = jest.spyOn(producer, "waitForDrain");
      waitForDrainSpy.mockResolvedValue(undefined);

      mockChannel.sendToQueue
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      const result = await producer.publishBatch("test-queue", messages);

      expect(result.successful).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors[0].error.message).toBe(
        "Buffer still full after waiting for drain",
      );
    });

    it("should throw RabbitPublishError on channel error", async () => {
      mockChannel.assertQueue.mockRejectedValue(new Error("Connection lost"));

      await expect(
        producer.publishBatch("test-queue", messages),
      ).rejects.toThrow(RabbitPublishError);
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
      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-exchange",
        "fanout",
        { durable: true },
      );
    });

    it("should not assert exchange on subsequent publishes", async () => {
      await producer.publishToExchange("test-exchange", "fanout", { id: 1 });
      await producer.publishToExchange("test-exchange", "fanout", { id: 2 });

      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(1);
    });

    it("should use routing key when provided", async () => {
      await producer.publishToExchange(
        "test-exchange",
        "topic",
        { id: 1 },
        { routingKey: "user.created" },
      );

      expect(mockChannel.publish).toHaveBeenCalledWith(
        "test-exchange",
        "user.created",
        expect.any(Buffer),
        expect.any(Object),
      );
    });

    it("should use empty routing key by default", async () => {
      await producer.publishToExchange("test-exchange", "fanout", { id: 1 });

      expect(mockChannel.publish).toHaveBeenCalledWith(
        "test-exchange",
        "",
        expect.any(Buffer),
        expect.any(Object),
      );
    });

    it("should apply publish options", async () => {
      await producer.publishToExchange(
        "test-exchange",
        "topic",
        { id: 1 },
        {
          routingKey: "test.key",
          persistent: false,
          expiration: "60000",
          priority: 5,
        },
      );

      expect(mockChannel.publish).toHaveBeenCalledWith(
        "test-exchange",
        "test.key",
        expect.any(Buffer),
        {
          persistent: false,
          expiration: "60000",
          priority: 5,
        },
      );
    });

    it("should throw RabbitPublishError on failure", async () => {
      mockChannel.publish.mockImplementation(() => {
        throw new Error("Exchange error");
      });

      await expect(
        producer.publishToExchange("test-exchange", "fanout", { id: 1 }),
      ).rejects.toThrow(RabbitPublishError);
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

    it("should use same routing key for all messages", async () => {
      await producer.publishBatchToExchange(
        "test-exchange",
        "topic",
        messages,
        { routingKey: "batch.key" },
      );

      expect(mockChannel.publish).toHaveBeenCalledTimes(3);
      for (let i = 0; i < messages.length; i++) {
        expect(mockChannel.publish).toHaveBeenCalledWith(
          "test-exchange",
          "batch.key",
          expect.any(Buffer),
          expect.any(Object),
        );
      }
    });

    it("should handle failures in batch to exchange", async () => {
      mockChannel.publish
        .mockReturnValueOnce(true)
        .mockImplementationOnce(() => {
          throw new Error("Failed");
        })
        .mockReturnValueOnce(true);

      const result = await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        messages,
      );

      expect(result.successful).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].index).toBe(1);
    });

    it("should return empty result for empty batch to exchange", async () => {
      const result = await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        [],
      );

      expect(result).toEqual({
        total: 0,
        successful: 0,
        failed: 0,
        errors: [],
      });
      expect(mockChannel.publish).not.toHaveBeenCalled();
    });

    it("should wait for drain when buffer is full in exchange batch", async () => {
      const waitForDrainSpy = jest.spyOn(producer, "waitForDrain");
      waitForDrainSpy.mockResolvedValue(undefined);

      mockChannel.publish
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        messages,
      );

      expect(waitForDrainSpy).toHaveBeenCalledTimes(1);
    });

    it("should retry after drain in exchange batch", async () => {
      const waitForDrainSpy = jest.spyOn(producer, "waitForDrain");
      waitForDrainSpy.mockResolvedValue(undefined);

      mockChannel.publish
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        messages,
      );

      expect(mockChannel.publish).toHaveBeenCalledTimes(4);
    });

    it("should mark as failed if retry still fails in exchange batch", async () => {
      const waitForDrainSpy = jest.spyOn(producer, "waitForDrain");
      waitForDrainSpy.mockResolvedValue(undefined);

      mockChannel.publish
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(false)
        .mockReturnValueOnce(true)
        .mockReturnValueOnce(true);

      const result = await producer.publishBatchToExchange(
        "test-exchange",
        "fanout",
        messages,
      );

      expect(result.failed).toBe(1);
      expect(result.errors[0].error.message).toBe(
        "Buffer still full after waiting for drain",
      );
    });

    it("should throw RabbitPublishError on channel error in exchange batch", async () => {
      mockChannel.assertExchange.mockRejectedValue(
        new Error("Connection lost"),
      );

      await expect(
        producer.publishBatchToExchange("test-exchange", "fanout", messages),
      ).rejects.toThrow(RabbitPublishError);
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
