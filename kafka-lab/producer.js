
const { Kafka } = require("kafkajs");

// 1. Configure connection to our Kafka broker
const kafka = new Kafka({
  clientId: "instagram-activity-app",
  brokers: ["localhost:9092"],
});

// 2. Create a producer
const producer = kafka.producer();

async function main() {
  // 3. Connect to Kafka
  await producer.connect();

  console.log("Connected to Kafka!");

  // 4. Simulate 10 different users
  for (let i = 1; i <= 10; i++) {

    const event = {
      eventId: `event-${i}`,
      userId: `user-${i}`,
      action: i % 2 === 0 ? "LIKE" : "LOGIN",
      postId: `post-${100 + i}`,
      timestamp: new Date().toISOString(),
    };

    // 5. Send this event to Kafka
    const result = await producer.send({
      topic: "user-events",
      messages: [
        {
          key: event.userId,
          value: JSON.stringify(event),
        },
      ],
    });

    console.log(
      `Sent: ${event.userId} | Action: ${event.action} | Partition: ${result[0].partition} | Offset: ${result[0].baseOffset}`
    );
  }

  // 6. Disconnect after sending all events
  await producer.disconnect();

  console.log("All events sent successfully!");
}

main().catch(async (error) => {
  console.error("Producer failed:", error);
  await producer.disconnect();
  process.exit(1);
});
