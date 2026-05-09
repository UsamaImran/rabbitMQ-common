import { vi } from "vitest";

export const mockChannel = {
  assertQueue: vi.fn(),
  sendToQueue: vi.fn(() => true),
  consume: vi.fn(),
  ack: vi.fn(),
  nack: vi.fn(),
  prefetch: vi.fn(),
};
