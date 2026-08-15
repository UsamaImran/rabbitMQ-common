import { Logger, RecoveryOptions } from "../types.js";

export class RecoveryManager {
  private isRecovering = false;
  private recoverRetries = 0;
  private readonly maxRecoverRetries: number;
  private readonly backoffBase: number;
  private readonly maxBackoff: number;
  private logger?: Logger;

  constructor(options: RecoveryOptions = {}, logger?: Logger) {
    this.maxRecoverRetries = options.maxRecoverRetries ?? -1;
    this.backoffBase = options.backoffBase ?? 1000;
    this.maxBackoff = options.maxBackoff ?? 30000;
    this.logger = logger;
  }

  /**
   * Check if recovery should be attempted
   * Returns true if we're under the retry limit
   */
  canRecover(): boolean {
    if (this.isRecovering) {
      this.logger?.warn("[RabbitMQ] Recovery already in progress");
      return false;
    }

    // Check if we've already used all retries
    if (
      this.maxRecoverRetries !== -1 &&
      this.recoverRetries >= this.maxRecoverRetries
    ) {
      this.logger?.error(
        `[RabbitMQ] Max recovery retries (${this.recoverRetries}) reached. Giving up.`,
      );
      return false;
    }

    return true;
  }

  /**
   * Start recovery attempt - increments retry count
   */
  startRecovery(): void {
    // Only increment if we're allowed to recover
    if (
      this.maxRecoverRetries === -1 ||
      this.recoverRetries < this.maxRecoverRetries
    ) {
      this.recoverRetries++;
      this.isRecovering = true;
      this.logger?.warn(
        `[RabbitMQ] Starting recovery attempt ${this.recoverRetries}`,
      );
    } else {
      this.logger?.error(
        `[RabbitMQ] Cannot start recovery: max retries (${this.maxRecoverRetries}) reached`,
      );
    }
  }

  /**
   * Get the next recovery delay using exponential backoff
   * When no retries yet (recoverRetries = 0), returns backoffBase
   * After first retry (recoverRetries = 1), returns backoffBase * 2
   */
  getNextDelay(): number {
    return Math.min(
      Math.pow(2, this.recoverRetries) * this.backoffBase,
      this.maxBackoff,
    );
  }

  /**
   * Complete recovery (successful)
   */
  completeRecovery(): void {
    this.isRecovering = false;
    this.logger?.info("[RabbitMQ] Recovery completed successfully");
  }

  /**
   * Reset recovery state (for successful initial connection)
   */
  reset(): void {
    this.recoverRetries = 0;
    this.isRecovering = false;
  }

  /**
   * Get current retry count
   */
  getRetryCount(): number {
    return this.recoverRetries;
  }

  /**
   * Check if currently recovering
   */
  isRecoveringNow(): boolean {
    return this.isRecovering;
  }
}
