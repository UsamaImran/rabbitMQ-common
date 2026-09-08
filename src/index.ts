export { Producer } from "./producer/index.js";
export { Consumer } from "./consumer/index.js";
export { ConnectionManager } from "./connectionManager.js";
export { BaseRabbit } from "./baseRabbit.js";
export { ExchangeManager } from "./exchangeManager.js";

export type { BaseRabbitOptions } from "./baseRabbit.js";
export type { ConsumerOptions } from "./consumer/index.js";

export type {
  RabbitConnectionError,
  RabbitPublishError,
  RabbitConsumeError,
  BatchPublishError,
} from "./types.js";

export type {
  Logger,
  ConsumeOptions,
  PublishOptions,
  QueueOptions,
  ExchangeType,
  ExchangePublishOptions,
  ExchangeConsumeOptions,
  ExchangeBindOptions,
  BatchPublishResult,
  Binding,
  RecoveryOptions,
} from "./types.js";

export type { ChannelModel, Channel, ConsumeMessage } from "amqplib";
