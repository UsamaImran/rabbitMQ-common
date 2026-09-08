import type { Logger, RecoveryOptions } from "../types.js";

export class RecoveryManager {
  private isRecovering = false;
  private recoverRetries = 0;
  private readonly maxRecoverRetries: number;
  private readonly backoffBase: number;
  private readonly maxBackoff: number;
  private readonly jitter: number;
  private logger?: Logger;

  constructor(options: RecoveryOptions = {}, logger?: Logger) {
    this.maxRecoverRetries = options.maxRecoverRetries ?? -1;
    this.backoffBase = options.backoffBase ?? 1000;
    this.maxBackoff = options.maxBackoff ?? 30000;
    this.jitter = Math.max(0, options.jitter ?? 0.2);
    this.logger = logger;
  }

  canRecover(): boolean {
    if (this.isRecovering) return false;
    return this.maxRecoverRetries === -1 || this.recoverRetries < this.maxRecoverRetries;
  }

  startRecovery(): void {
    if (!this.canRecover()) return;
    this.recoverRetries++;
    this.isRecovering = true;
  }

  getNextDelay(): number {
    const exponential = Math.min(
      Math.pow(2, Math.max(0, this.recoverRetries - 1)) * this.backoffBase,
      this.maxBackoff,
    );
    const spread = exponential * this.jitter;
    return Math.round(exponential - spread + Math.random() * spread * 2);
  }

  completeRecovery(): void {
    this.isRecovering = false;
  }

  reset(): void {
    this.recoverRetries = 0;
    this.isRecovering = false;
  }

  getRetryCount(): number {
    return this.recoverRetries;
  }

  isRecoveringNow(): boolean {
    return this.isRecovering;
  }
}
