
const amqp = require("amqplib");

async function sendMessage() {
  const connection = await amqp.connect("amqp://localhost");
  const channel = await connection.createChannel();

  const queue = "orders";
  const message = "New order received: Order #101";

  await channel.assertQueue(queue, {
    durable: true
  });

  channel.sendToQueue(queue, Buffer.from(message), {
    persistent: true
  });

  console.log("Sent:", message);

  await channel.close();
  await connection.close();
}

sendMessage().catch(console.error);
