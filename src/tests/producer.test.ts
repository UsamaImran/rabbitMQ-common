import { describe, it, expect, vi, beforeEach } from "vitest";
import { Producer } from "../producer.js";
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

describe("Producer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("publishes message with defaults", async () => {
    const producer = new Producer("amqp://localhost");

    await producer.publish("emails", { hello: "world" });

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", {
      durable: true,
    });

    expect(mockChannel.sendToQueue).toHaveBeenCalled();
  });

  it("respects options", async () => {
    const producer = new Producer("amqp://localhost");

    await producer.publish(
      "emails",
      { hello: "world" },
      {
        durable: false,
        persistent: false,
      },
    );

    expect(mockChannel.assertQueue).toHaveBeenCalledWith("emails", {
      durable: false,
    });

    expect(mockChannel.sendToQueue).toHaveBeenCalledWith(
      "emails",
      expect.any(Buffer),
      { persistent: false },
    );
  });
});
