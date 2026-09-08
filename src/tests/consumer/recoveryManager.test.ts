// @ts-nocheck
import { describe, it, expect } from "@jest/globals";
import { RecoveryManager } from "../../consumer/recoveryManager.js";

describe("RecoveryManager", () => {
  it("starts with no retries", () => {
    const manager = new RecoveryManager();
    expect(manager.getRetryCount()).toBe(0);
    expect(manager.isRecoveringNow()).toBe(false);
    expect(manager.canRecover()).toBe(true);
  });

  it("increments retries and prevents concurrent recovery", () => {
    const manager = new RecoveryManager({ maxRecoverRetries: 2, jitter: 0 });
    manager.startRecovery();
    expect(manager.getRetryCount()).toBe(1);
    expect(manager.canRecover()).toBe(false);
    manager.completeRecovery();
    manager.startRecovery();
    expect(manager.getRetryCount()).toBe(2);
    manager.completeRecovery();
    expect(manager.canRecover()).toBe(false);
  });

  it("uses exponential backoff starting at the configured base", () => {
    const manager = new RecoveryManager({ backoffBase: 1000, jitter: 0 });
    expect(manager.getNextDelay()).toBe(1000);
    manager.startRecovery();
    expect(manager.getNextDelay()).toBe(1000);
    manager.completeRecovery();
    manager.startRecovery();
    expect(manager.getNextDelay()).toBe(2000);
  });

  it("caps backoff at maxBackoff", () => {
    const manager = new RecoveryManager({ backoffBase: 1000, maxBackoff: 5000, jitter: 0 });
    for (let i = 0; i < 10; i++) {
      manager.startRecovery();
      manager.completeRecovery();
    }
    expect(manager.getNextDelay()).toBe(5000);
  });

  it("supports bounded and infinite retries", () => {
    const bounded = new RecoveryManager({ maxRecoverRetries: 1, jitter: 0 });
    bounded.startRecovery();
    bounded.completeRecovery();
    expect(bounded.canRecover()).toBe(false);

    const infinite = new RecoveryManager({ maxRecoverRetries: -1, jitter: 0 });
    for (let i = 0; i < 20; i++) {
      expect(infinite.canRecover()).toBe(true);
      infinite.startRecovery();
      infinite.completeRecovery();
    }
  });

  it("adds bounded jitter when configured", () => {
    const manager = new RecoveryManager({ backoffBase: 1000, jitter: 0.2 });
    const delay = manager.getNextDelay();
    expect(delay).toBeGreaterThanOrEqual(800);
    expect(delay).toBeLessThanOrEqual(1200);
  });

  it("resets retry state", () => {
    const manager = new RecoveryManager({ jitter: 0 });
    manager.startRecovery();
    manager.completeRecovery();
    manager.reset();
    expect(manager.getRetryCount()).toBe(0);
    expect(manager.isRecoveringNow()).toBe(false);
  });
});
