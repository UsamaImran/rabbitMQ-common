// @ts-nocheck
import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { QueueAssertor } from "../../producer/queueAssertor";

describe("QueueAssertor", () => {
  let queueAssertor: QueueAssertor;
  let mockChannel: any;

  beforeEach(() => {
    queueAssertor = new QueueAssertor();
    mockChannel = {
      assertQueue: jest.fn().mockResolvedValue({ queue: "test-queue" }),
    };
  });

  describe("assertQueue", () => {
    it("should assert queue with default options", async () => {
      await queueAssertor.assertQueue(mockChannel, "test-queue");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: true,
      });
    });

    it("should assert queue with custom options", async () => {
      await queueAssertor.assertQueue(mockChannel, "test-queue", {
        durable: false,
        maxLength: 100,
        messageTtl: 5000,
        priority: 10,
      });

      expect(mockChannel.assertQueue).toHaveBeenCalledWith("test-queue", {
        durable: false,
        maxLength: 100,
        messageTtl: 5000,
        maxPriority: 10,
      });
    });

    it("should cache queue and not assert twice", async () => {
      await queueAssertor.assertQueue(mockChannel, "test-queue");
      await queueAssertor.assertQueue(mockChannel, "test-queue");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(1);
    });

    it("should assert different queues separately", async () => {
      await queueAssertor.assertQueue(mockChannel, "queue-1");
      await queueAssertor.assertQueue(mockChannel, "queue-2");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(2);
    });

    it("should re-assert queue after cache reset", async () => {
      await queueAssertor.assertQueue(mockChannel, "test-queue");
      queueAssertor.resetCache("test-queue");
      await queueAssertor.assertQueue(mockChannel, "test-queue");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(2);
    });

    it("should reset all queues when called without args", async () => {
      await queueAssertor.assertQueue(mockChannel, "queue-1");
      await queueAssertor.assertQueue(mockChannel, "queue-2");

      queueAssertor.resetCache();

      await queueAssertor.assertQueue(mockChannel, "queue-1");
      await queueAssertor.assertQueue(mockChannel, "queue-2");

      expect(mockChannel.assertQueue).toHaveBeenCalledTimes(4);
    });

    it("should check if queue is asserted", async () => {
      expect(queueAssertor.isAsserted("test-queue")).toBe(false);

      await queueAssertor.assertQueue(mockChannel, "test-queue");

      expect(queueAssertor.isAsserted("test-queue")).toBe(true);
    });
  });
});
