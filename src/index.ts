export { Producer } from "./producer/index";
export { Consumer } from "./consumer/index";
export { ConnectionManager } from "./connectionManager.js";
export { BaseRabbit } from "./baseRabbit.js";
export { ExchangeManager } from "./exchangeManager.js";

export type { BaseRabbitOptions } from "./baseRabbit.js";

// Errors
export {
  RabbitConnectionError,
  RabbitPublishError,
  RabbitConsumeError,
} from "./types.js";

// Interfaces & option types
export type {
  Logger,
  ConsumeOptions,
  PublishOptions,
  QueueOptions,
  ExchangeType,
  ExchangePublishOptions,
  ExchangeConsumeOptions,
  ExchangeBindOptions,
} from "./types.js";

// Re-export amqplib types
export type { ChannelModel, Channel, ConsumeMessage } from "amqplib";
