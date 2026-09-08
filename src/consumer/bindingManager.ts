import type { Channel } from "amqplib";
import type { Binding, ExchangeType } from "../types.js";
import { ExchangeManager } from "../exchangeManager.js";

export class BindingManager {
  private bindings = new Map<string, Binding>();
  private exchangeManager = new ExchangeManager();

  async bind(
    channel: Channel,
    queue: string,
    exchange: string,
    exchangeType: ExchangeType,
    routingKey = "",
  ): Promise<void> {
    this.exchangeManager.resetForChannel(channel);
    await this.exchangeManager.assertExchange(channel, exchange, exchangeType);
    await channel.bindQueue(queue, exchange, routingKey);

    const binding = { queue, exchange, exchangeType, routingKey };
    this.bindings.set(this.getBindingKey(binding), binding);
  }

  async unbind(
    channel: Channel,
    queue: string,
    exchange: string,
    routingKey = "",
  ): Promise<void> {
    await channel.unbindQueue(queue, exchange, routingKey);
    this.bindings.delete(this.getBindingKey({ queue, exchange, exchangeType: "direct", routingKey }));
    // Remove by queue/exchange/routing key regardless of exchange type.
    for (const [key, binding] of this.bindings) {
      if (binding.queue === queue && binding.exchange === exchange && binding.routingKey === routingKey) {
        this.bindings.delete(key);
      }
    }
  }

  async restore(channel: Channel): Promise<void> {
    for (const binding of this.bindings.values()) {
      await this.exchangeManager.assertExchange(
        channel,
        binding.exchange,
        binding.exchangeType,
      );
      await channel.bindQueue(binding.queue, binding.exchange, binding.routingKey);
    }
  }

  hasBinding(queue: string, exchange: string, routingKey = ""): boolean {
    return Array.from(this.bindings.values()).some(
      (binding) => binding.queue === queue && binding.exchange === exchange && binding.routingKey === routingKey,
    );
  }

  getActiveBindings(): string[] {
    return Array.from(this.bindings.values()).map(
      ({ queue, exchange, routingKey }) => `${queue}:${exchange}:${routingKey}`,
    );
  }

  clear(): void {
    this.bindings.clear();
  }

  resetExchangeCache(exchange?: string, type?: ExchangeType): void {
    this.exchangeManager.resetExchangeCache(exchange, type);
  }

  private getBindingKey(binding: Binding): string {
    return `${binding.queue}\u0000${binding.exchange}\u0000${binding.routingKey}`;
  }
}
