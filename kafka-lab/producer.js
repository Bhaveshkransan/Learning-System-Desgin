
const { Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "instagram-producer",
  brokers: ["localhost:9092"],
});

const producer = kafka.producer();

async function main() {
  const numberOfUsers = 1000;
  const topic = "user-events";

  const partitionCounts = {};

  try {
    await producer.connect();
    console.log("Connected to Kafka!");

    const startTime = Date.now();

    for (let i = 1; i <= numberOfUsers; i++) {
      const userId = `user-${i}`;

      const event = {
        userId,
        action: i % 2 === 0 ? "LIKE" : "LOGIN",
        postId: `post-${100 + i}`,
        timestamp: new Date().toISOString(),
      };

      const result = await producer.send({
        topic,
        messages: [
          {
            key: userId,
            value: JSON.stringify(event),
          },
        ],
      });

      const partition = result[0].partition;

      partitionCounts[partition] =
        (partitionCounts[partition] || 0) + 1;

      if (i % 100 === 0) {
        console.log(`${i} users' events sent`);
      }
    }

    const duration = Date.now() - startTime;

    console.log("\n========== FINAL REPORT ==========");
    console.log("Total users:", numberOfUsers);
    console.log("Total events:", numberOfUsers);
    console.log("Partition distribution:", partitionCounts);
    console.log("Time taken:", duration, "ms");
    console.log("==================================");

  } catch (error) {
    console.error("Producer error:", error);
  } finally {
    await producer.disconnect();
  }
}

main();
