// @ts-nocheck
import { jest, describe, it, expect, beforeEach } from "@jest/globals";
import { ExchangeManager } from "../exchangeManager.js";

describe("ExchangeManager", () => {
  let exchangeManager: ExchangeManager;
  let mockChannel: any;

  beforeEach(() => {
    exchangeManager = new ExchangeManager();
    mockChannel = {
      assertExchange: jest.fn().mockResolvedValue(undefined),
    };
  });

  describe("assertExchange", () => {
    it("should assert exchange on first call", async () => {
      await exchangeManager.assertExchange(
        mockChannel,
        "test-exchange",
        "fanout",
      );
      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-exchange",
        "fanout",
        {
          durable: true,
        },
      );
    });

    it("should not assert exchange on subsequent calls", async () => {
      await exchangeManager.assertExchange(
        mockChannel,
        "test-exchange",
        "fanout",
      );
      await exchangeManager.assertExchange(
        mockChannel,
        "test-exchange",
        "fanout",
      );
      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(1);
    });

    it("should treat different types as separate", async () => {
      await exchangeManager.assertExchange(
        mockChannel,
        "test-exchange",
        "fanout",
      );
      await exchangeManager.assertExchange(
        mockChannel,
        "test-exchange",
        "topic",
      );
      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(2);
    });

    it("should use custom durable option", async () => {
      await exchangeManager.assertExchange(
        mockChannel,
        "test-exchange",
        "direct",
        {
          durable: false,
        },
      );
      expect(mockChannel.assertExchange).toHaveBeenCalledWith(
        "test-exchange",
        "direct",
        {
          durable: false,
        },
      );
    });
  });

  describe("resetExchangeCache", () => {
    beforeEach(async () => {
      await exchangeManager.assertExchange(mockChannel, "exchange1", "fanout");
      await exchangeManager.assertExchange(mockChannel, "exchange1", "topic");
      await exchangeManager.assertExchange(mockChannel, "exchange2", "fanout");
    });

    it("should reset specific exchange+type", () => {
      exchangeManager.resetExchangeCache("exchange1", "fanout");
      // Call again to verify cache was cleared
      exchangeManager.assertExchange(mockChannel, "exchange1", "fanout");
      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(4);
    });

    it("should reset all caches", () => {
      exchangeManager.resetExchangeCache();
      exchangeManager.assertExchange(mockChannel, "exchange1", "fanout");
      expect(mockChannel.assertExchange).toHaveBeenCalledTimes(4);
    });
  });
});
