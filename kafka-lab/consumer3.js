
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "instagram-consumer-3",
  brokers: ["localhost:9092"],
});

const consumer = kafka.consumer({
  groupId: "activity-processing-group",
});

async function main() {
  await consumer.connect();

  console.log("Consumer 3 connected!");

  await consumer.subscribe({
    topic: "user-events",
    fromBeginning: true,
  });

  console.log("Consumer 3 waiting for events...");

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const event = JSON.parse(message.value.toString());

      console.log("--------------------------------");
      console.log("Processed by: Consumer 3");
      console.log("Topic:", topic);
      console.log("Partition:", partition);
      console.log("Offset:", message.offset);
      console.log("User:", event.userId);
      console.log("Action:", event.action);
      console.log("--------------------------------");
    },
  });
}

main().catch(console.error);
