// @ts-nocheck
import { describe, it, expect, beforeEach, jest } from "@jest/globals";
import { BatchHandler } from "../../producer/batchHandler.js";

describe("BatchHandler", () => {
  let handler: BatchHandler;
  let channel: any;
  let waitForDrain: jest.Mock;

  beforeEach(() => {
    handler = new BatchHandler();
    waitForDrain = jest.fn().mockResolvedValue(undefined);
    channel = {
      sendToQueue: jest.fn().mockReturnValue(true),
      publish: jest.fn().mockReturnValue(true),
    };
  });

  it("publishes every queue message exactly once", async () => {
    channel.sendToQueue.mockReturnValueOnce(false).mockReturnValueOnce(true).mockReturnValueOnce(true);

    const result = await handler.publishBatch(
      channel,
      "orders",
      [{ id: 1 }, { id: 2 }, { id: 3 }],
      {},
      waitForDrain,
    );

    expect(waitForDrain).toHaveBeenCalledTimes(1);
    expect(channel.sendToQueue).toHaveBeenCalledTimes(3);
    expect(result).toEqual({ total: 3, successful: 3, failed: 0, errors: [] });
  });

  it("does not republish a message after backpressure", async () => {
    channel.sendToQueue.mockReturnValueOnce(false).mockReturnValueOnce(true);

    await handler.publishBatch(channel, "orders", [{ id: 1 }, { id: 2 }], {}, waitForDrain);

    expect(channel.sendToQueue).toHaveBeenCalledTimes(2);
    expect(channel.sendToQueue.mock.calls[0][1]).toEqual(Buffer.from('{"id":1}'));
    expect(channel.sendToQueue.mock.calls[1][1]).toEqual(Buffer.from('{"id":2}'));
  });

  it("records synchronous send failures without aborting the batch", async () => {
    channel.sendToQueue
      .mockReturnValueOnce(true)
      .mockImplementationOnce(() => { throw new Error("failed"); })
      .mockReturnValueOnce(true);

    const result = await handler.publishBatch(
      channel,
      "orders",
      [{ id: 1 }, { id: 2 }, { id: 3 }],
      {},
      waitForDrain,
    );

    expect(result.successful).toBe(2);
    expect(result.failed).toBe(1);
    expect(result.errors[0].index).toBe(1);
  });

  it("applies the same backpressure semantics to exchanges", async () => {
    channel.publish.mockReturnValueOnce(false).mockReturnValueOnce(true);

    const result = await handler.publishBatchToExchange(
      channel,
      "events",
      "orders.created",
      [{ id: 1 }, { id: 2 }],
      {},
      waitForDrain,
    );

    expect(waitForDrain).toHaveBeenCalledTimes(1);
    expect(channel.publish).toHaveBeenCalledTimes(2);
    expect(result.failed).toBe(0);
  });
});
