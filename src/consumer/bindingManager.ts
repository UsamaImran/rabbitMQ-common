// src/consumer/BindingManager.ts

import type { Channel } from "amqplib";

import type { ExchangeType } from "../types.js";
import { ExchangeManager } from "../exchangeManager.js";

export interface Binding {
  queue: string;
  exchange: string;
  routingKey: string;
}

export class BindingManager {
  private bindings = new Set<string>();
  private exchangeManager = new ExchangeManager();

  /**
   * Bind a queue to an exchange
   */
  async bind(
    channel: Channel,
    queue: string,
    exchange: string,
    exchangeType: ExchangeType,
    routingKey: string = "",
  ): Promise<void> {
    // Assert exchange exists
    await this.exchangeManager.assertExchange(channel, exchange, exchangeType);

    // Bind queue to exchange
    await channel.bindQueue(queue, exchange, routingKey);

    // Track binding
    const bindingKey = this.getBindingKey(queue, exchange, routingKey);
    this.bindings.add(bindingKey);
  }

  /**
   * Unbind a queue from an exchange
   */
  async unbind(
    channel: Channel,
    queue: string,
    exchange: string,
    routingKey: string = "",
  ): Promise<void> {
    await channel.unbindQueue(queue, exchange, routingKey);

    const bindingKey = this.getBindingKey(queue, exchange, routingKey);
    this.bindings.delete(bindingKey);
  }

  /**
   * Check if a binding exists
   */
  hasBinding(
    queue: string,
    exchange: string,
    routingKey: string = "",
  ): boolean {
    const bindingKey = this.getBindingKey(queue, exchange, routingKey);
    return this.bindings.has(bindingKey);
  }

  /**
   * Get all active bindings
   */
  getActiveBindings(): string[] {
    return Array.from(this.bindings);
  }

  /**
   * Clear all tracked bindings
   */
  clear(): void {
    this.bindings.clear();
  }

  /**
   * Get binding key for tracking
   */
  private getBindingKey(
    queue: string,
    exchange: string,
    routingKey: string,
  ): string {
    return `${queue}:${exchange}:${routingKey}`;
  }

  /**
   * Reset exchange cache
   */
  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    this.exchangeManager.resetExchangeCache(exchange, type);
  }
}
