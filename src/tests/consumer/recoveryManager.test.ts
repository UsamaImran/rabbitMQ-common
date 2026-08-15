import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { RecoveryManager } from "../../../src/consumer/recoveryManager.js";

describe("RecoveryManager", () => {
  let mockLogger: any;

  beforeEach(() => {
    mockLogger = {
      info: jest.fn(),
      warn: jest.fn(),
      error: jest.fn(),
    };
  });

  describe("constructor", () => {
    it("should use default options", () => {
      const manager = new RecoveryManager();

      expect(manager.getRetryCount()).toBe(0);
      expect(manager.isRecoveringNow()).toBe(false);
    });

    it("should use custom options", () => {
      const manager = new RecoveryManager({
        maxRecoverRetries: 5,
        backoffBase: 2000,
        maxBackoff: 60000,
      });

      // Should still work with defaults for untested values
      expect(manager.getRetryCount()).toBe(0);
    });

    it("should use provided logger", () => {
      const manager = new RecoveryManager({}, mockLogger);
      // Just verify it was created with the logger
      expect(manager).toBeInstanceOf(RecoveryManager);
    });
  });

  describe("canRecover", () => {
    it("should allow recovery initially", () => {
      const manager = new RecoveryManager({ maxRecoverRetries: 3 });
      expect(manager.canRecover()).toBe(true);
    });

    it("should allow recovery within retry limit", () => {
      const manager = new RecoveryManager({ maxRecoverRetries: 3 });

      manager.startRecovery();
      expect(manager.canRecover()).toBe(true);

      manager.startRecovery();
      expect(manager.canRecover()).toBe(true);

      manager.startRecovery();
      expect(manager.canRecover()).toBe(false); // Hit limit
    });

    it("should not allow recovery when already recovering", () => {
      const manager = new RecoveryManager();

      // Manually set isRecovering to true
      manager.startRecovery();

      expect(manager.canRecover()).toBe(false);
    });

    it("should not exceed max retries", () => {
      const manager = new RecoveryManager({ maxRecoverRetries: 2 });

      expect(manager.canRecover()).toBe(true);
      manager.startRecovery();
      expect(manager.canRecover()).toBe(true);
      manager.startRecovery();
      expect(manager.canRecover()).toBe(false);
    });

    it("should allow infinite retries when maxRecoverRetries is -1", () => {
      const manager = new RecoveryManager({ maxRecoverRetries: -1 });

      for (let i = 0; i < 10; i++) {
        expect(manager.canRecover()).toBe(true);
        manager.startRecovery();
      }
    });

    it("should log when recovery not possible due to retry limit", () => {
      const logger = mockLogger;
      const manager = new RecoveryManager({ maxRecoverRetries: 1 }, logger);

      manager.startRecovery();
      manager.canRecover();

      expect(logger.error).toHaveBeenCalledWith(
        expect.stringContaining("Max recovery retries"),
      );
    });

    it("should log when recovery already in progress", () => {
      const logger = mockLogger;
      const manager = new RecoveryManager({}, logger);

      manager.startRecovery();
      manager.canRecover();

      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining("Recovery already in progress"),
      );
    });
  });

  describe("startRecovery", () => {
    it("should increment retry count", () => {
      const manager = new RecoveryManager();

      expect(manager.getRetryCount()).toBe(0);
      manager.startRecovery();
      expect(manager.getRetryCount()).toBe(1);
      manager.startRecovery();
      expect(manager.getRetryCount()).toBe(2);
    });

    it("should set isRecovering to true", () => {
      const manager = new RecoveryManager();

      expect(manager.isRecoveringNow()).toBe(false);
      manager.startRecovery();
      expect(manager.isRecoveringNow()).toBe(true);
    });

    it("should log recovery start", () => {
      const logger = mockLogger;
      const manager = new RecoveryManager({}, logger);

      manager.startRecovery();

      expect(logger.warn).toHaveBeenCalledWith(
        expect.stringContaining("Starting recovery attempt"),
      );
    });
  });

  describe("getNextDelay", () => {
    it("should use exponential backoff", () => {
      const manager = new RecoveryManager({ backoffBase: 1000 });

      manager.startRecovery();
      expect(manager.getNextDelay()).toBe(1000);

      manager.startRecovery();
      expect(manager.getNextDelay()).toBe(2000);

      manager.startRecovery();
      expect(manager.getNextDelay()).toBe(4000);
    });

    it("should cap backoff at maxBackoff", () => {
      const manager = new RecoveryManager({
        backoffBase: 1000,
        maxBackoff: 5000,
      });

      // Simulate many retries
      for (let i = 0; i < 10; i++) {
        manager.startRecovery();
      }

      expect(manager.getNextDelay()).toBe(5000);
    });

    it("should use default maxBackoff of 30000", () => {
      const manager = new RecoveryManager({ backoffBase: 10000 });

      // Simulate many retries
      for (let i = 0; i < 10; i++) {
        manager.startRecovery();
      }

      expect(manager.getNextDelay()).toBe(30000);
    });

    it("should use default backoffBase of 1000", () => {
      const manager = new RecoveryManager();

      manager.startRecovery();
      expect(manager.getNextDelay()).toBe(1000);
    });
  });

  describe("completeRecovery", () => {
    it("should set isRecovering to false", () => {
      const manager = new RecoveryManager();

      manager.startRecovery();
      expect(manager.isRecoveringNow()).toBe(true);

      manager.completeRecovery();
      expect(manager.isRecoveringNow()).toBe(false);
    });

    it("should log recovery completion", () => {
      const logger = mockLogger;
      const manager = new RecoveryManager({}, logger);

      manager.startRecovery();
      manager.completeRecovery();

      expect(logger.info).toHaveBeenCalledWith(
        expect.stringContaining("Recovery completed successfully"),
      );
    });
  });

  describe("reset", () => {
    it("should reset retry count to 0", () => {
      const manager = new RecoveryManager();

      manager.startRecovery();
      manager.startRecovery();
      expect(manager.getRetryCount()).toBe(2);

      manager.reset();
      expect(manager.getRetryCount()).toBe(0);
    });

    it("should set isRecovering to false", () => {
      const manager = new RecoveryManager();

      manager.startRecovery();
      expect(manager.isRecoveringNow()).toBe(true);

      manager.reset();
      expect(manager.isRecoveringNow()).toBe(false);
    });
  });
});
