import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { MessageSender } from "../../producer/messageSender";

describe("MessageSender", () => {
  let sender: MessageSender;
  let mockChannel: any;

  beforeEach(() => {
    sender = new MessageSender();
    mockChannel = {
      sendToQueue: jest.fn().mockReturnValue(true),
      publish: jest.fn().mockReturnValue(true),
    };
  });

  describe("sendToQueue", () => {
    const queue = "test-queue";
    const message = { id: 1, name: "test" };

    it("should send message as JSON", () => {
      sender.sendToQueue(mockChannel, queue, message);

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        queue,
        Buffer.from(JSON.stringify(message)),
        expect.objectContaining({ persistent: true }),
      );
    });

    it("should use default persistent: true", () => {
      sender.sendToQueue(mockChannel, queue, message);

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        queue,
        expect.any(Buffer),
        { persistent: true },
      );
    });

    it("should apply custom publish options", () => {
      sender.sendToQueue(mockChannel, queue, message, {
        persistent: false,
        expiration: "60000",
        priority: 5,
      });

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

    it("should return the result from channel", () => {
      mockChannel.sendToQueue.mockReturnValue(false);
      const result = sender.sendToQueue(mockChannel, queue, message);

      expect(result).toBe(false);
    });

    it("should handle non-object messages", () => {
      sender.sendToQueue(mockChannel, queue, "string message");

      expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
        queue,
        Buffer.from(JSON.stringify("string message")),
        expect.any(Object),
      );
    });
  });

  describe("publishToExchange", () => {
    const exchange = "test-exchange";
    const routingKey = "test.key";
    const message = { id: 1, name: "test" };

    it("should publish message to exchange", () => {
      sender.publishToExchange(mockChannel, exchange, routingKey, message);

      expect(mockChannel.publish).toHaveBeenCalledWith(
        exchange,
        routingKey,
        Buffer.from(JSON.stringify(message)),
        expect.objectContaining({ persistent: true }),
      );
    });

    it("should use default persistent: true", () => {
      sender.publishToExchange(mockChannel, exchange, routingKey, message);

      expect(mockChannel.publish).toHaveBeenCalledWith(
        exchange,
        routingKey,
        expect.any(Buffer),
        { persistent: true },
      );
    });

    it("should apply custom publish options", () => {
      sender.publishToExchange(mockChannel, exchange, routingKey, message, {
        persistent: false,
        expiration: "60000",
        priority: 5,
      });

      expect(mockChannel.publish).toHaveBeenCalledWith(
        exchange,
        routingKey,
        expect.any(Buffer),
        {
          persistent: false,
          expiration: "60000",
          priority: 5,
        },
      );
    });

    it("should return the result from channel", () => {
      mockChannel.publish.mockReturnValue(false);
      const result = sender.publishToExchange(
        mockChannel,
        exchange,
        routingKey,
        message,
      );

      expect(result).toBe(false);
    });

    it("should handle empty routing key", () => {
      sender.publishToExchange(mockChannel, exchange, "", message);

      expect(mockChannel.publish).toHaveBeenCalledWith(
        exchange,
        "",
        expect.any(Buffer),
        expect.any(Object),
      );
    });
  });
});
