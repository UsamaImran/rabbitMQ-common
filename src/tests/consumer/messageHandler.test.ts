//@ts-nocheck

import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { MessageHandler } from "../../../src/consumer/messageHandler.js";
import type { ConsumeMessage } from "amqplib";

describe("MessageHandler", () => {
  let mockChannel: any;
  let mockCallbacks: any;
  let messageHandler: MessageHandler<any>;

  beforeEach(() => {
    mockChannel = {
      ack: jest.fn(),
      nack: jest.fn(),
    };

    mockCallbacks = {
      onMessage: jest.fn().mockResolvedValue(undefined),
      onError: jest.fn().mockResolvedValue(undefined),
      logger: {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      },
    };

    messageHandler = new MessageHandler(mockChannel, mockCallbacks, false);
  });

  describe("processMessage", () => {
    it("should process valid message successfully", async () => {
      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1, name: "test" })),
        properties: { correlationId: "corr-123" },
      } as ConsumeMessage;

      await messageHandler.processMessage(testMessage);

      expect(mockCallbacks.onMessage).toHaveBeenCalledWith(
        { id: 1, name: "test" },
        testMessage,
      );
      expect(mockChannel.ack).toHaveBeenCalledWith(testMessage);
      expect(mockChannel.nack).not.toHaveBeenCalled();
    });

    it("should handle malformed JSON", async () => {
      const testMessage = {
        content: Buffer.from("invalid json"),
        properties: {},
      } as ConsumeMessage;

      await messageHandler.processMessage(testMessage);

      expect(mockCallbacks.onMessage).not.toHaveBeenCalled();
      expect(mockCallbacks.onError).toHaveBeenCalled();
      expect(mockCallbacks.onError.mock.calls[0][0].message).toContain(
        "Failed to parse message",
      );
      expect(mockChannel.nack).toHaveBeenCalledWith(testMessage, false, false);
    });

    it("should handle onMessage error and requeue when DLQ disabled", async () => {
      mockCallbacks.onMessage.mockRejectedValue(new Error("Processing error"));

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1 })),
        properties: {},
      } as ConsumeMessage;

      await messageHandler.processMessage(testMessage);

      expect(mockCallbacks.onError).toHaveBeenCalled();
      expect(mockChannel.nack).toHaveBeenCalledWith(testMessage, false, true);
    });

    it("should handle onMessage error and not requeue when DLQ enabled", async () => {
      const handlerWithDLQ = new MessageHandler(
        mockChannel,
        mockCallbacks,
        true,
      );

      mockCallbacks.onMessage.mockRejectedValue(new Error("Processing error"));

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1 })),
        properties: {},
      } as ConsumeMessage;

      await handlerWithDLQ.processMessage(testMessage);

      expect(mockCallbacks.onError).toHaveBeenCalled();
      expect(mockChannel.nack).toHaveBeenCalledWith(testMessage, false, false);
    });

    it("should handle onError throwing", async () => {
      mockCallbacks.onMessage.mockRejectedValue(new Error("Processing error"));
      mockCallbacks.onError.mockRejectedValue(
        new Error("Error handler failed"),
      );

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1 })),
        properties: {},
      } as ConsumeMessage;

      await messageHandler.processMessage(testMessage);

      expect(mockCallbacks.logger.error).toHaveBeenCalledWith(
        expect.stringContaining("onError handler threw"),
      );
    });

    it("should handle non-Error objects", async () => {
      mockCallbacks.onMessage.mockRejectedValue("String error");

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1 })),
        properties: {},
      } as ConsumeMessage;

      await messageHandler.processMessage(testMessage);

      expect(mockCallbacks.onError).toHaveBeenCalled();
      expect(mockCallbacks.onError.mock.calls[0][0].message).toBe(
        "String error",
      );
    });

    it("should ignore null messages", async () => {
      await messageHandler.processMessage(null as any);

      expect(mockCallbacks.onMessage).not.toHaveBeenCalled();
      expect(mockChannel.ack).not.toHaveBeenCalled();
      expect(mockChannel.nack).not.toHaveBeenCalled();
    });
  });

  describe("createHandler", () => {
    it("should create a handler function", () => {
      const handler = messageHandler.createHandler();
      expect(typeof handler).toBe("function");
    });

    it("should call processMessage for non-null messages", async () => {
      const processSpy = jest.spyOn(messageHandler, "processMessage");
      const handler = messageHandler.createHandler();

      const testMessage = {
        content: Buffer.from(JSON.stringify({ id: 1 })),
        properties: {},
      } as ConsumeMessage;

      await handler(testMessage);

      expect(processSpy).toHaveBeenCalledWith(testMessage);
    });

    it("should not call processMessage for null messages", async () => {
      const processSpy = jest.spyOn(messageHandler, "processMessage");
      const handler = messageHandler.createHandler();

      await handler(null);

      expect(processSpy).not.toHaveBeenCalled();
    });
  });
});
