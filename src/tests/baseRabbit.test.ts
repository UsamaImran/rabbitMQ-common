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
const mockClose = jest.fn();

jest.mock("../connectionManager.js", () => ({
  ConnectionManager: {
    getConnection: mockGetConnection,
    isConnected: mockIsConnected,
    close: mockClose,
  },
}));

import { BaseRabbit } from "../baseRabbit.js";

// Create a concrete implementation for testing
class TestRabbit extends BaseRabbit {
  async testGetChannel() {
    return this.getChannel();
  }
}

describe("BaseRabbit", () => {
  const testUrl = "amqp://localhost:5672";
  let mockChannel: any;

  beforeEach(() => {
    mockChannel = {
      close: jest.fn().mockResolvedValue(undefined),
      on: jest.fn(),
    };

    mockGetConnection.mockResolvedValue({
      createChannel: jest.fn().mockResolvedValue(mockChannel),
    });
    mockIsConnected.mockReturnValue(false);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  describe("constructor", () => {
    it("should use default maxRetries = 5", () => {
      const rabbit = new TestRabbit(testUrl);
      expect(rabbit["maxRetries"]).toBe(5);
    });

    it("should use custom maxRetries", () => {
      const rabbit = new TestRabbit(testUrl, { maxRetries: 10 });
      expect(rabbit["maxRetries"]).toBe(10);
    });

    it("should use custom logger", () => {
      const customLogger = {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      };
      const rabbit = new TestRabbit(testUrl, { logger: customLogger });
      expect(rabbit["logger"]).toBe(customLogger);
    });
  });

  describe("getChannel", () => {
    it("should create and cache channel", async () => {
      const rabbit = new TestRabbit(testUrl);
      const channel1 = await rabbit.testGetChannel();
      const channel2 = await rabbit.testGetChannel();

      expect(mockGetConnection).toHaveBeenCalledTimes(1);
      expect(channel1).toBe(channel2);
    });

    it("should set up error and close handlers", async () => {
      const rabbit = new TestRabbit(testUrl);
      await rabbit.testGetChannel();

      expect(mockChannel.on).toHaveBeenCalledWith(
        "error",
        expect.any(Function),
      );
      expect(mockChannel.on).toHaveBeenCalledWith(
        "close",
        expect.any(Function),
      );
    });
  });

  describe("close", () => {
    it("should close channel and clear reference", async () => {
      const rabbit = new TestRabbit(testUrl);
      await rabbit.testGetChannel();
      expect(rabbit["channel"]).toBe(mockChannel);

      await rabbit.close();
      expect(mockChannel.close).toHaveBeenCalled();
      expect(rabbit["channel"]).toBeUndefined();
    });

    it("should handle close when no channel exists", async () => {
      const rabbit = new TestRabbit(testUrl);
      await expect(rabbit.close()).resolves.not.toThrow();
    });

    it("should ignore close errors", async () => {
      mockChannel.close.mockRejectedValue(new Error("Already closed"));
      const rabbit = new TestRabbit(testUrl);
      await rabbit.testGetChannel();

      await expect(rabbit.close()).resolves.not.toThrow();
    });
  });

  describe("isConnected", () => {
    it("should delegate to ConnectionManager", () => {
      const rabbit = new TestRabbit(testUrl);
      rabbit.isConnected();
      expect(mockIsConnected).toHaveBeenCalledWith(testUrl);
    });
  });

  describe("isChannelReady", () => {
    it("should return false when no channel", () => {
      const rabbit = new TestRabbit(testUrl);
      expect(rabbit.isChannelReady()).toBe(false);
    });

    it("should return true when channel exists", async () => {
      const rabbit = new TestRabbit(testUrl);
      await rabbit.testGetChannel();
      // Skip this test for now - fix the implementation
      expect(rabbit.isChannelReady()).toBe(true);
    });
  });

  describe("getUrl", () => {
    it("should return the URL", () => {
      const rabbit = new TestRabbit(testUrl);
      expect(rabbit.getUrl()).toBe(testUrl);
    });
  });
});
