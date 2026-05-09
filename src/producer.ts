import { BaseRabbit } from "./baseRabbit.js";

export class Producer extends BaseRabbit {
  async publish<T>(
    queue: string,
    message: T,
    options: {
      durable?: boolean;
      persistent?: boolean;
    } = {},
  ) {
    if (!this.channel) {
      await this.init();
    }

    await this.channel!.assertQueue(queue, {
      durable: options.durable ?? true,
    });

    return this.channel!.sendToQueue(
      queue,
      Buffer.from(JSON.stringify(message)),
      {
        persistent: options.persistent ?? true,
      },
    );
  }
}
