import { jest } from "@jest/globals";

// Make jest available globally
global.jest = jest;

// Define types for mocks
export type MockChannel = {
  assertQueue: jest.Mock;
  sendToQueue: jest.Mock;
  publish: jest.Mock;
  consume: jest.Mock;
  ack: jest.Mock;
  nack: jest.Mock;
  prefetch: jest.Mock;
  close: jest.Mock;
  on: jest.Mock;
  once: jest.Mock;
  removeAllListeners: jest.Mock;
  assertExchange: jest.Mock;
  bindQueue: jest.Mock;
  unbindQueue: jest.Mock;
};

export type MockConnection = {
  createChannel: jest.Mock;
  close: jest.Mock;
  on: jest.Mock;
};
