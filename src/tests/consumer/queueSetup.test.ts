// @ts-nocheck

import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { QueueSetup } from "../../consumer/queueSetup.js";

describe("QueueSetup", () => {
  let queueSetup: QueueSetup;
  let mockChannel: any;

  beforeEach(() => {
    queueSetup = new QueueSetup();
    mockChannel = {
      assertQueue: jest.fn().mockResolvedValue(undefined),
      assertExchange: jest.fn().mockResolvedValue(undefined),
      bindQueue: jest.fn().mockResolvedValue(undefined),
    } as any;
  });

  describe("setupQueue", () => {
    it("should setup queue without DLQ with default options", async () => {
      await queueSetup.setupQueue(mockChannel, "test-queue");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: true,
      });
      expect(mockChannel.assertExchange).not.toHaveBeenCalled();
      expect(mockChannel.bindQueue).not.toHaveBeenCalled();
    });

    it("should setup queue without DLQ with custom options", async () => {
      await queueSetup.setupQueue(mockChannel, "test-queue", {
        queueOptions: {
          durable: false,
          maxLength: 100,
          messageTtl: 5000,
          priority: 10,
        },
      });

      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: false,
        maxLength: 100,
        messageTtl: 5000,
        maxPriority: 10,
      });
    });

    it("should setup queue with DLQ", async () => {
      await queueSetup.setupQueue(mockChannel, "test-queue", { useDLQ: true });

      // Should create DLX exchange
      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-queue_dlx",
        "direct",
        { durable: true },
      );

      // Should create DLQ with default durable: true
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(
        "test-queue_failed",
        {
          durable: true,
        },
      );

      // Should bind DLQ to DLX
      expect(mockChannel.bindQueue).toHaveBeenCalledWith(
        "test-queue_failed",
        "test-queue_dlx",
        "dead-letter",
      );

      // Should create main queue with DLQ config
      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: true,
        deadLetterExchange: "test-queue_dlx",
        deadLetterRoutingKey: "dead-letter",
      });
    });

    it("should setup queue with DLQ and custom options", async () => {
      await queueSetup.setupQueue(mockChannel, "test-queue", {
        useDLQ: true,
        queueOptions: {
          durable: false,
          maxLength: 50,
          messageTtl: 10000,
        },
      });

      // DLQ should get the same durable option
      expect(mockChannel.assertQueue).toHaveBeenCalledWith(
        "test-queue_failed",
        {
          durable: false,
          maxLength: 50,
          messageTtl: 10000,
        },
      );

      // Main queue should get options plus DLQ config
      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: false,
        maxLength: 50,
        messageTtl: 10000,
        deadLetterExchange: "test-queue_dlx",
        deadLetterRoutingKey: "dead-letter",
      });
    });

    it("should cache queue setup and not setup twice", async () => {
      await queueSetup.setupQueue(mockChannel, "test-queue");
      await queueSetup.setupQueue(mockChannel, "test-queue");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should setup different queues separately", async () => {
      await queueSetup.setupQueue(mockChannel, "queue-1");
      await queueSetup.setupQueue(mockChannel, "queue-2");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(2);
    });

    it("should re-setup queue after cache reset", async () => {
      await queueSetup.setupQueue(mockChannel, "test-queue");
      queueSetup.resetCache("test-queue");
      await queueSetup.setupQueue(mockChannel, "test-queue");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(2);
    });

    it("should reset all queues when called without args", async () => {
      await queueSetup.setupQueue(mockChannel, "queue-1");
      await queueSetup.setupQueue(mockChannel, "queue-2");

      queueSetup.resetCache();

      await queueSetup.setupQueue(mockChannel, "queue-1");
      await queueSetup.setupQueue(mockChannel, "queue-2");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(4);
    });

    it("should check if queue is setup", async () => {
      expect(queueSetup.isSetUp("test-queue")).toBe(false);

      await queueSetup.setupQueue(mockChannel, "test-queue");

      expect(queueSetup.isSetUp("test-queue")).toBe(true);
    });
  });
});
