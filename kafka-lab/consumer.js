
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: `instagram-consumer-${process.pid}`,
  brokers: ["localhost:9092"],
});

// Create a consumer belonging to a consumer group
const consumer = kafka.consumer({
  groupId: "activity-processing-group",
});

async function main() {
  await consumer.connect();

  console.log("Consumer connected!");

  // Subscribe to our existing topic
  await consumer.subscribe({
    topic: "user-events",
    fromBeginning: true,
  });

  console.log("Waiting for events...\n");

  // Start processing messages
  await consumer.run({

   eachMessage: async ({ topic, partition, message }) => {

  const event = JSON.parse(message.value.toString());

  console.log("--------------------------------");
  console.log("Processed by:", process.pid);
  console.log("Topic:", topic);
  console.log("Partition:", partition);
  console.log("Offset:", message.offset);
  console.log("User:", event.userId);
  console.log("Action:", event.action);
  console.log("Post:", event.postId);
  console.log("--------------------------------\n");
}
  });
}

main().catch(async (error) => {
  console.error("Consumer failed:", error);
  await consumer.disconnect();
  process.exit(1);
});
