import { describe, it, expect, vi, beforeEach } from "vitest";
import { Consumer } from "../consumer.js";
import { mockChannel } from "./mock.js";

vi.mock("amqplib", () => ({
  default: {
    connect: vi.fn(() =>
      Promise.resolve({
        createChannel: () => Promise.resolve(mockChannel),
        on: vi.fn(),
      }),
    ),
  },
}));

type Msg = { hello: string };

class TestConsumer extends Consumer<Msg> {
  async onMessage(data: Msg) {
    expect(data.hello).toBe("world");
  }
}

describe("Consumer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("consumes messages and acks", async () => {
    const consumer = new TestConsumer("amqp://localhost");

    mockChannel.consume.mockImplementation((_q, cb) => {
      cb({
        content: Buffer.from(JSON.stringify({ hello: "world" })),
      });
    });

    await consumer.consume("emails");

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", {
      durable: true,
    });

    expect(mockChannel.ack).toHaveBeenCalled();
  });

  it("nacks on error", async () => {
    class BadConsumer extends Consumer<Msg> {
      async onMessage() {
        throw new Error("fail");
      }
    }

    const consumer = new BadConsumer("amqp://localhost");

    mockChannel.consume.mockImplementation((_q, cb) => {
      cb({
        content: Buffer.from(JSON.stringify({ hello: "world" })),
      });
    });

    await consumer.consume("emails");

    expect(mockChannel.nack).toHaveBeenCalled();
  });
});
