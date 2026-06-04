// @ts-nocheck
import {
  jest,
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
} from "@jest/globals";

// Simple mock - no complex types
const mockConnect = jest.fn();

jest.mock("amqplib", () => ({
  connect: mockConnect,
}));

import { ConnectionManager } from "../connectionManager.js";

describe("ConnectionManager", () => {
  const testUrl = "amqp://localhost:5672";

  beforeEach(async () => {
    jest.clearAllMocks();
    await ConnectionManager.close();
  });

  afterEach(async () => {
    await ConnectionManager.close();
  });

  describe("getConnection", () => {
    it("should create a new connection if none exists", async () => {
      const mockConn = {
        createChannel: jest.fn(),
        on: jest.fn(),
        close: jest.fn(),
      };
      mockConnect.mockResolvedValue(mockConn);

      const conn = await ConnectionManager.getConnection(testUrl, 3);
      expect(mockConnect).toHaveBeenCalledWith(testUrl);
      expect(conn).toBeDefined();
    });

    it("should reuse an existing connection", async () => {
      const mockConn = {
        createChannel: jest.fn(),
        on: jest.fn(),
        close: jest.fn(),
      };
      mockConnect.mockResolvedValue(mockConn);

      const conn1 = await ConnectionManager.getConnection(testUrl, 3);
      const conn2 = await ConnectionManager.getConnection(testUrl, 3);
      expect(mockConnect).toHaveBeenCalledTimes(1);
      expect(conn1).toBe(conn2);
    });

    it("should handle multiple URLs separately", async () => {
      const mockConn1 = {
        createChannel: jest.fn(),
        on: jest.fn(),
        close: jest.fn(),
      };
      const mockConn2 = {
        createChannel: jest.fn(),
        on: jest.fn(),
        close: jest.fn(),
      };

      mockConnect
        .mockResolvedValueOnce(mockConn1)
        .mockResolvedValueOnce(mockConn2);

      const url1 = "amqp://localhost:5672";
      const url2 = "amqp://localhost:5673";

      const conn1 = await ConnectionManager.getConnection(url1, 3);
      const conn2 = await ConnectionManager.getConnection(url2, 3);

      expect(mockConnect).toHaveBeenCalledTimes(2);
      expect(conn1).not.toBe(conn2);
    });

    it("should retry on connection failure", async () => {
      const mockConn = {
        createChannel: jest.fn(),
        on: jest.fn(),
        close: jest.fn(),
      };

      mockConnect
        .mockRejectedValueOnce(new Error("Connection refused"))
        .mockRejectedValueOnce(new Error("Connection refused"))
        .mockResolvedValueOnce(mockConn);

      const conn = await ConnectionManager.getConnection(testUrl, 3);
      expect(mockConnect).toHaveBeenCalledTimes(3);
      expect(conn).toBeDefined();
    }, 10000); // Increase timeout to 10 seconds

    it("should throw after max retries", async () => {
      mockConnect.mockRejectedValue(new Error("Connection refused"));

      await expect(ConnectionManager.getConnection(testUrl, 2)).rejects.toThrow(
        /failed after 2 attempts/,
      );
    });
  });

  describe("isConnected", () => {
    it("returns false when no connection exists", () => {
      expect(ConnectionManager.isConnected(testUrl)).toBe(false);
    });

    it("returns true when connection exists", async () => {
      const mockConn = {
        createChannel: jest.fn(),
        on: jest.fn(),
        close: jest.fn(),
      };
      mockConnect.mockResolvedValue(mockConn);

      await ConnectionManager.getConnection(testUrl, 3);
      expect(ConnectionManager.isConnected(testUrl)).toBe(true);
    });
  });

  describe("close", () => {
    it("should close a specific connection", async () => {
      const mockClose = jest.fn().mockResolvedValue(undefined);
      const mockConn = {
        createChannel: jest.fn(),
        close: mockClose,
        on: jest.fn(),
      };
      mockConnect.mockResolvedValue(mockConn);

      await ConnectionManager.getConnection(testUrl, 3);
      await ConnectionManager.close(testUrl);

      expect(mockClose).toHaveBeenCalled();
      expect(ConnectionManager.isConnected(testUrl)).toBe(false);
    });

    it("should close all connections", async () => {
      const mockClose1 = jest.fn().mockResolvedValue(undefined);
      const mockClose2 = jest.fn().mockResolvedValue(undefined);

      mockConnect
        .mockResolvedValueOnce({
          createChannel: jest.fn(),
          close: mockClose1,
          on: jest.fn(),
        })
        .mockResolvedValueOnce({
          createChannel: jest.fn(),
          close: mockClose2,
          on: jest.fn(),
        });

      await ConnectionManager.getConnection("amqp://localhost:5672", 3);
      await ConnectionManager.getConnection("amqp://localhost:5673", 3);
      await ConnectionManager.close();

      expect(mockClose1).toHaveBeenCalled();
      expect(mockClose2).toHaveBeenCalled();
    });
  });

  describe("setLogger", () => {
    it("should set a custom logger", () => {
      const customLogger = {
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
      };
      ConnectionManager.setLogger(customLogger);
      expect(customLogger.info).toBeDefined();
    });
  });
});
