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

import { Producer } from "../producer.js";
import { RabbitPublishError } from "../types.js";

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
    // Initialize channel
    await producer["getChannel"]();
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("publish", () => {
    const queue = "test-queue";
    const message = { id: 1, name: "test" };

    it("should assert queue on first publish", async () => {
      await producer.publish(queue, message);
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(queue, {
        durable: true,
      });
    });

    it("should not assert queue on subsequent publishes", async () => {
      await producer.publish(queue, message);
      await producer.publish(queue, message);
      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should send message as JSON", async () => {
      await producer.publish(queue, message);
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        queue,
        Buffer.from(JSON.stringify(message)),
        expect.objectContaining({ persistent: true }),
      );
    });

    it("should apply queue options", async () => {
      await producer.publish(
        queue,
        message,
        {},
        { durable: false, maxLength: 100, messageTtl: 5000, priority: 10 },
      );
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(queue, {
        durable: false,
        maxLength: 100,
        messageTtl: 5000,
        maxPriority: 10,
      });
    });

    it("should apply publish options", async () => {
      await producer.publish(
        queue,
        message,
        { persistent: false, expiration: "60000", priority: 5 },
        {},
      );
      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        queue,
        expect.any(Buffer),
        {
          persistent: false,
          expiration: "60000",
          priority: 5,
        },
      );
    });

    it("should throw RabbitPublishError on failure", async () => {
      mockChannel.sendToQueue.mockImplementation(() => {
        throw new Error("Channel closed");
      });

      await expect(producer.publish(queue, message)).rejects.toThrow(
        RabbitPublishError,
      );
    });

    it("should return false when buffer is full", async () => {
      mockChannel.sendToQueue.mockReturnValue(false);
      const result = await producer.publish(queue, message);
      expect(result).toBe(false);
    });
  });

  describe("publishToExchange", () => {
    const exchange = "test-exchange";
    const message = { id: 1, name: "test" };

    it("should assert exchange on first publish", async () => {
      await producer.publishToExchange(exchange, "fanout", message);
      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        exchange,
        "fanout",
        {
          durable: true,
        },
      );
    });

    it("should not assert exchange on subsequent publishes", async () => {
      await producer.publishToExchange(exchange, "fanout", message);
      await producer.publishToExchange(exchange, "fanout", message);
      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(1);
    });

    it("should publish with empty routing key for fanout by default", async () => {
      await producer.publishToExchange(exchange, "fanout", message);
      expect(mockChannel.publish).toHaveBeenCalledWith(
        exchange,
        "",
        expect.any(Buffer),
        expect.any(Object),
      );
    });

    it("should use custom routing key", async () => {
      await producer.publishToExchange(exchange, "topic", message, {
        routingKey: "user.created",
      });
      expect(mockChannel.publish).toHaveBeenCalledWith(
        exchange,
        "user.created",
        expect.any(Buffer),
        expect.any(Object),
      );
    });
  });

  describe("waitForDrain", () => {
    it("should resolve when drain event fires", async () => {
      mockChannel.once.mockImplementation((event: string, cb: () => void) => {
        if (event === "drain") {
          cb();
        }
      });

      const drainPromise = producer.waitForDrain();
      await expect(drainPromise).resolves.toBeUndefined();
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
