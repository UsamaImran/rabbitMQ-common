export const mockChannel = {
  assertQueue: jest.fn().mockResolvedValue({ queue: "test-queue" }),
  sendToQueue: jest.fn().mockReturnValue(true),
  publish: jest.fn().mockReturnValue(true),
  consume: jest.fn(),
  ack: jest.fn(),
  nack: jest.fn(),
  prefetch: jest.fn().mockResolvedValue(undefined),
  close: jest.fn().mockResolvedValue(undefined),
  on: jest.fn(),
  once: jest.fn(),
  removeAllListeners: jest.fn(),
  assertExchange: jest.fn().mockResolvedValue(undefined),
  bindQueue: jest.fn().mockResolvedValue(undefined),
  unbindQueue: jest.fn().mockResolvedValue(undefined),
};

export const mockConnection = {
  createChannel: jest.fn().mockResolvedValue(mockChannel),
  close: jest.fn().mockResolvedValue(undefined),
  on: jest.fn(),
};

const amqplib = {
  connect: jest.fn().mockResolvedValue(mockConnection),
};

export default amqplib;
