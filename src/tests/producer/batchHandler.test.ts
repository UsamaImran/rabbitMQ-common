// @ts-nocheck
import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { BatchHandler } from "../../producer/batchHandler";

describe("BatchHandler", () => {
  let batchHandler: BatchHandler;
  let mockChannel: any;
  let mockWaitForDrain: jest.Mock;

  beforeEach(() => {
    batchHandler = new BatchHandler();
    mockWaitForDrain = jest.fn().mockResolvedValue(undefined);
    mockChannel = {
      sendToQueue: jest.fn().mockReturnValue(true),
      publish: jest.fn().mockReturnValue(true),
    };
  });

  describe("publishBatch", () => {
    const queue = "test-queue";
    const messages = [
      { id: 1, name: "test1" },
      { id: 2, name: "test2" },
      { id: 3, name: "test3" },
    ];

    it("should return empty result for empty messages array", async () => {
      const result = await batchHandler.publishBatch(
        mockChannel,
        queue,
        [],
        {},
        mockWaitForDrain,
      );

      expect(result).toEqual({
        total: 0,
        successful: 0,
        failed: 0,
        errors: [],
      });
      expect(mockChannel.sendToQueue).not.toHaveBeenCalled();
    });

    it("should publish all messages successfully", async () => {
      const result = await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(3);
      expect(result.total).toBe(3);
      expect(result.successful).toBe(3);
      expect(result.failed).toBe(0);
      expect(result.errors).toEqual([]);
    });

    it("should apply publish options to all messages", async () => {
      await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        { persistent: false, expiration: "60000", priority: 5 },
        mockWaitForDrain,
      );

      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(3);
      for (let i = 0; i < messages.length; i++) {
        expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
          queue,
          expect.any(Buffer),
          {
            persistent: false,
            expiration: "60000",
            priority: 5,
          },
        );
      }
    });

    it("should track individual message failures", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(true)
        .mockImplementationOnce(() => {
          throw new Error("Failed to publish");
        })
        .mockReturnValueOnce(true);

      const result = await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(result.total).toBe(3);
      expect(result.successful).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors).toHaveLength(1);
      expect(result.errors[0].index).toBe(1);
      expect(result.errors[0].message).toEqual(messages[1]);
      expect(result.errors[0].error.message).toBe("Failed to publish");
    });

    it("should handle multiple failures", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(true)
        .mockImplementationOnce(() => {
          throw new Error("Failed 1");
        })
        .mockImplementationOnce(() => {
          throw new Error("Failed 2");
        });

      const result = await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(result.total).toBe(3);
      expect(result.successful).toBe(1);
      expect(result.failed).toBe(2);
      expect(result.errors).toHaveLength(2);
      expect(result.errors[0].index).toBe(1);
      expect(result.errors[1].index).toBe(2);
    });

    it("should wait for drain when buffer is full", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(false) // First message - buffer full
        .mockReturnValueOnce(true) // Retry success
        .mockReturnValueOnce(true) // Second message
        .mockReturnValueOnce(true); // Third message

      await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(mockWaitForDrain).toHaveBeenCalledTimes(1);
    });

    it("should retry after drain when buffer is full", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(false) // First message - buffer full
        .mockReturnValueOnce(true) // Retry success
        .mockReturnValueOnce(true) // Second message
        .mockReturnValueOnce(true); // Third message

      await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      // First message called twice (original + retry), others once
      expect(mockChannel.sendToQueue).toHaveBeenCalledTimes(4);
    });

    it("should mark message as failed if retry still fails", async () => {
      mockChannel.sendToQueue
        .mockReturnValueOnce(false) // First message - buffer full
        .mockReturnValueOnce(false) // Retry still fails
        .mockReturnValueOnce(true) // Second message
        .mockReturnValueOnce(true); // Third message

      const result = await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(result.total).toBe(3);
      expect(result.successful).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors[0].index).toBe(0);
      expect(result.errors[0].error.message).toBe(
        "Buffer still full after waiting for drain",
      );
    });

    it("should handle non-Error objects in catch", async () => {
      mockChannel.sendToQueue.mockImplementation(() => {
        throw "String error"; // Non-Error throw
      });

      const result = await batchHandler.publishBatch(
        mockChannel,
        queue,
        [{ id: 1 }],
        {},
        mockWaitForDrain,
      );

      expect(result.failed).toBe(1);
      expect(result.errors[0].error.message).toBe("String error");
    });

    it("should serialize messages as JSON", async () => {
      await batchHandler.publishBatch(
        mockChannel,
        queue,
        messages,
        {},
        mockWaitForDrain,
      );

      for (let i = 0; i < messages.length; i++) {
        expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
          queue,
          Buffer.from(JSON.stringify(messages[i])),
          expect.any(Object),
        );
      }
    });
  });

  describe("publishBatchToExchange", () => {
    const exchange = "test-exchange";
    const routingKey = "test.key";
    const messages = [
      { id: 1, name: "test1" },
      { id: 2, name: "test2" },
      { id: 3, name: "test3" },
    ];

    it("should return empty result for empty messages array", async () => {
      const result = await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        routingKey,
        [],
        {},
        mockWaitForDrain,
      );

      expect(result).toEqual({
        total: 0,
        successful: 0,
        failed: 0,
        errors: [],
      });
      expect(mockChannel.publish).not.toHaveBeenCalled();
    });

    it("should publish all messages to exchange", async () => {
      const result = await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        routingKey,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(mockChannel.publish).toHaveBeenCalledTimes(3);
      expect(result.total).toBe(3);
      expect(result.successful).toBe(3);
      expect(result.failed).toBe(0);
    });

    it("should use same routing key for all messages", async () => {
      await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        "custom.key",
        messages,
        {},
        mockWaitForDrain,
      );

      expect(mockChannel.publish).toHaveBeenCalledTimes(3);
      for (let i = 0; i < messages.length; i++) {
        expect(mockChannel.publish).toHaveBeenCalledWith(
          exchange,
          "custom.key",
          expect.any(Buffer),
          expect.any(Object),
        );
      }
    });

    it("should track individual failures in exchange batch", async () => {
      mockChannel.publish
        .mockReturnValueOnce(true)
        .mockImplementationOnce(() => {
          throw new Error("Failed to publish");
        })
        .mockReturnValueOnce(true);

      const result = await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        routingKey,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(result.total).toBe(3);
      expect(result.successful).toBe(2);
      expect(result.failed).toBe(1);
      expect(result.errors[0].index).toBe(1);
      expect(result.errors[0].message).toEqual(messages[1]);
    });

    it("should wait for drain when buffer is full in exchange batch", async () => {
      mockChannel.publish
        .mockReturnValueOnce(false) // First message - buffer full
        .mockReturnValueOnce(true) // Retry success
        .mockReturnValueOnce(true) // Second message
        .mockReturnValueOnce(true); // Third message

      await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        routingKey,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(mockWaitForDrain).toHaveBeenCalledTimes(1);
    });

    it("should retry after drain in exchange batch", async () => {
      mockChannel.publish
        .mockReturnValueOnce(false) // First message - buffer full
        .mockReturnValueOnce(true) // Retry success
        .mockReturnValueOnce(true) // Second message
        .mockReturnValueOnce(true); // Third message

      await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        routingKey,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(mockChannel.publish).toHaveBeenCalledTimes(4);
    });

    it("should mark as failed if retry still fails in exchange batch", async () => {
      mockChannel.publish
        .mockReturnValueOnce(false) // First message - buffer full
        .mockReturnValueOnce(false) // Retry still fails
        .mockReturnValueOnce(true) // Second message
        .mockReturnValueOnce(true); // Third message

      const result = await batchHandler.publishBatchToExchange(
        mockChannel,
        exchange,
        routingKey,
        messages,
        {},
        mockWaitForDrain,
      );

      expect(result.failed).toBe(1);
      expect(result.errors[0].error.message).toBe(
        "Buffer still full after waiting for drain",
      );
    });
  });
});
