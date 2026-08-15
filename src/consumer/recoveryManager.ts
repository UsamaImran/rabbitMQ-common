import type { Logger } from "../types.js";

export interface RecoveryOptions {
  maxRecoverRetries?: number;
  backoffBase?: number;
  maxBackoff?: number;
}

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
   */
  canRecover(): boolean {
    if (this.isRecovering) {
      this.logger?.warn("[RabbitMQ] Recovery already in progress");
      return false;
    }

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
   * Get the next recovery delay using exponential backoff
   */
  getNextDelay(): number {
    return Math.min(
      Math.pow(2, this.recoverRetries) * this.backoffBase,
      this.maxBackoff,
    );
  }

  /**
   * Start recovery attempt
   */
  startRecovery(): void {
    this.isRecovering = true;
    this.recoverRetries++;
    this.logger?.warn(
      `[RabbitMQ] Starting recovery attempt ${this.recoverRetries}`,
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
