require("dotenv").config();
const amqp = require("amqplib");
const db = require("./db");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const QUEUE = process.env.QUEUE_NAME || "orders";
const POLL_INTERVAL_MS = parseInt(process.env.OUTBOX_POLL_INTERVAL_MS || "1000", 10);

let isRunning = true;
let connection;
let channel;

async function startOutboxPublisher() {
  await db.testConnection();

  connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createConfirmChannel();

  await channel.assertQueue(QUEUE, { durable: true });

  console.log(`\n======================================================`);
  console.log(` Transactional Outbox Publisher Started`);
  console.log(` Polling 'outbox_events' every ${POLL_INTERVAL_MS}ms -> publishing to '${QUEUE}'`);
  console.log(`======================================================\n`);

  async function pollAndPublish() {
    if (!isRunning) return;

    const client = await db.pool.connect();
    try {
      // Use transaction with FOR UPDATE SKIP LOCKED to prevent race conditions
      await client.query("BEGIN");

      const res = await client.query(
        `SELECT id, aggregate_id, event_type, payload, status, created_at 
         FROM outbox_events 
         WHERE status = 'PENDING' 
         ORDER BY created_at ASC 
         LIMIT 10 
         FOR UPDATE SKIP LOCKED;`
      );

      if (res.rows.length > 0) {
        console.log(`[OUTBOX] Found ${res.rows.length} PENDING event(s) to publish.`);

        for (const event of res.rows) {
          const payloadBuffer = Buffer.from(
            typeof event.payload === "string" ? event.payload : JSON.stringify(event.payload)
          );

          // Publish to RabbitMQ with publisher confirms
          channel.sendToQueue(QUEUE, payloadBuffer, {
            persistent: true,
            contentType: "application/json",
            messageId: event.id,
            headers: {
              "x-outbox-event-id": event.id,
              "x-aggregate-id": event.aggregate_id,
            },
          });

          await channel.waitForConfirms();
          console.log(`[RABBITMQ CONFIRMED] Published event ${event.id} (Order: ${event.aggregate_id})`);

          // TEST 4 Hook: Simulate Crash immediately after broker confirm but before DB update
          if (process.env.SIMULATE_PUBLISHER_CRASH_AFTER_CONFIRM === "true") {
            console.error(`\n💥 [SIMULATED CRASH] Crashing publisher after RabbitMQ confirm but BEFORE outbox DB update!`);
            await client.query("ROLLBACK");
            client.release();
            process.exit(42);
          }

          // Mark event as PUBLISHED in database
          await client.query(
            "UPDATE outbox_events SET status = 'PUBLISHED', published_at = NOW() WHERE id = $1",
            [event.id]
          );

          console.log(`[OUTBOX DB UPDATED] Event ${event.id} marked as PUBLISHED.`);
        }
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      console.error("[OUTBOX ERROR] Failed processing outbox batch:", err.message);
    } finally {
      client.release();
    }

    if (isRunning) {
      setTimeout(pollAndPublish, POLL_INTERVAL_MS);
    }
  }

  // Kick off poll loop
  pollAndPublish();

  const shutdown = async () => {
    console.log("\nStopping Outbox Publisher...");
    isRunning = false;
    if (channel) await channel.close();
    if (connection) await connection.close();
    await db.pool.end();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

startOutboxPublisher().catch((err) => {
  console.error("Outbox publisher failed to start:", err);
  process.exit(1);
});
