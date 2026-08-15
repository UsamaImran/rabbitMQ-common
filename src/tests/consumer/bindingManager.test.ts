//@ts-nocheck
import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { BindingManager } from "../../../src/consumer/bindingManager.js";

describe("BindingManager", () => {
  let bindingManager: BindingManager;
  let mockChannel: any;

  beforeEach(() => {
    bindingManager = new BindingManager();
    mockChannel = {
      bindQueue: jest.fn().mockResolvedValue(undefined),
      unbindQueue: jest.fn().mockResolvedValue(undefined),
      assertExchange: jest.fn().mockResolvedValue(undefined),
    } as any;
  });

  describe("bind", () => {
    it("should bind queue to exchange", async () => {
      await bindingManager.bind(
        mockChannel,
        "test-queue",
        "test-exchange",
        "topic",
        "test.key",
      );

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

    it("should bind with empty routing key", async () => {
      await bindingManager.bind(
        mockChannel,
        "test-queue",
        "test-exchange",
        "fanout",
      );

      expect(mockChannel.bindQueue).toHaveBeenCalledWith(
        "test-queue",
        "test-exchange",
        "",
      );
    });

    it("should track bindings", async () => {
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-1",
        "topic",
        "key-1",
      );
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-2",
        "topic",
        "key-2",
      );

      const bindings = bindingManager.getActiveBindings();
      expect(bindings).toHaveLength(2);
      expect(bindings).toContain("queue-1:exchange-1:key-1");
      expect(bindings).toContain("queue-1:exchange-2:key-2");
    });

    it("should not duplicate bindings", async () => {
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-1",
        "topic",
        "key-1",
      );
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-1",
        "topic",
        "key-1",
      );

      const bindings = bindingManager.getActiveBindings();
      expect(bindings).toHaveLength(1);
    });
  });

  describe("unbind", () => {
    it("should unbind queue from exchange", async () => {
      // First bind
      await bindingManager.bind(
        mockChannel,
        "test-queue",
        "test-exchange",
        "topic",
        "test.key",
      );

      expect(
        bindingManager.hasBinding("test-queue", "test-exchange", "test.key"),
      ).toBe(true);

      // Then unbind
      await bindingManager.unbind(
        mockChannel,
        "test-queue",
        "test-exchange",
        "test.key",
      );

      expect(
        bindingManager.hasBinding("test-queue", "test-exchange", "test.key"),
      ).toBe(false);
      expect(mockChannel.unbindQueue).toHaveBeenCalledWith(
        "test-queue",
        "test-exchange",
        "test.key",
      );
    });

    it("should unbind with empty routing key", async () => {
      await bindingManager.bind(
        mockChannel,
        "test-queue",
        "test-exchange",
        "fanout",
      );

      await bindingManager.unbind(
        mockChannel,
        "test-queue",
        "test-exchange",
        "",
      );

      expect(mockChannel.unbindQueue).toHaveBeenCalledWith(
        "test-queue",
        "test-exchange",
        "",
      );
    });
  });

  describe("hasBinding", () => {
    it("should return true for existing binding", async () => {
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-1",
        "topic",
        "key-1",
      );

      expect(bindingManager.hasBinding("queue-1", "exchange-1", "key-1")).toBe(
        true,
      );
    });

    it("should return false for non-existing binding", async () => {
      expect(bindingManager.hasBinding("queue-1", "exchange-1", "key-1")).toBe(
        false,
      );
    });
  });

  describe("getActiveBindings", () => {
    it("should return empty array when no bindings", () => {
      expect(bindingManager.getActiveBindings()).toEqual([]);
    });

    it("should return all active bindings", async () => {
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-1",
        "topic",
        "key-1",
      );
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-2",
        "topic",
        "key-2",
      );

      const bindings = bindingManager.getActiveBindings();
      expect(bindings).toHaveLength(2);
    });
  });

  describe("clear", () => {
    it("should clear all bindings", async () => {
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-1",
        "topic",
        "key-1",
      );
      await bindingManager.bind(
        mockChannel,
        "queue-1",
        "exchange-2",
        "topic",
        "key-2",
      );

      bindingManager.clear();
      expect(bindingManager.getActiveBindings()).toHaveLength(0);
    });
  });

  describe("resetExchangeCache", () => {
    it("should reset exchange cache for specific exchange", async () => {
      const resetSpy = jest.spyOn(
        bindingManager["exchangeManager"],
        "resetExchangeCache",
      );

      bindingManager.resetExchangeCache("test-exchange", "topic");

      expect(resetSpy).toHaveBeenCalledWith("test-exchange", "topic");
    });

    it("should reset all exchange cache when called without args", async () => {
      const resetSpy = jest.spyOn(
        bindingManager["exchangeManager"],
        "resetExchangeCache",
      );

      bindingManager.resetExchangeCache();

      expect(resetSpy).toHaveBeenCalledWith(undefined, undefined);
    });
  });
});
