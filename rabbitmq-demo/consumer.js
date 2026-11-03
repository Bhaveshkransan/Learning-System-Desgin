require("dotenv").config();
const amqp = require("amqplib");
const db = require("./db");

const RABBITMQ_URL = process.env.RABBITMQ_URL || "amqp://localhost:5672";
const QUEUE = process.env.QUEUE_NAME || "orders";
const DLQ = process.env.DLQ_NAME || "orders_dlq";
const MAX_RETRIES = 2;

let connection;
let channel;

async function startConsumer() {
  // 1. Verify PostgreSQL connection
  await db.testConnection();

  // 2. Connect to RabbitMQ
  connection = await amqp.connect(RABBITMQ_URL);
  channel = await connection.createChannel();

  // Ensure queues exist
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.assertQueue(DLQ, { durable: true });

  // Backpressure: Process only 1 unacknowledged message at a time
  channel.prefetch(1);

  console.log(`\n======================================================`);
  console.log(` Order Worker running. Waiting for messages in '${QUEUE}'`);
  console.log(` Dead-Letter Queue: '${DLQ}'`);
  console.log(`======================================================\n`);

  channel.consume(
    QUEUE,
    async (message) => {
      if (!message) return;

      const deliveryTag = message.fields.deliveryTag;
      const isRedelivered = message.fields.redelivered;
      const headers = message.properties.headers || {};
      const retryCount = (headers["x-retry-count"] || 0) + (isRedelivered ? 1 : 0);

      let order;

      // -------------------------------------------------------------
      // Step 1: Safe Deserialization (Poison Pill Protection)
      // -------------------------------------------------------------
      try {
        order = JSON.parse(message.content.toString());
      } catch (parseError) {
        console.error(
          `[POISON PILL] Failed to parse message content. Routing to ${DLQ}:`,
          message.content.toString()
        );

        // Forward corrupt message to DLQ so queue is not blocked
        channel.sendToQueue(DLQ, message.content, {
          persistent: true,
          headers: {
            "x-death-reason": "JSON_PARSE_ERROR",
            "x-original-queue": QUEUE,
            "x-failed-at": new Date().toISOString(),
          },
        });

        // Acknowledge original so it's removed from main queue
        channel.ack(message);
        return;
      }

      const orderId = order.id || order.orderId;
      console.log(`\n--> [Message Received] Order ID: ${orderId} (Retry count: ${retryCount})`);

      try {
        // -------------------------------------------------------------
        // Step 2: Idempotency Check in PostgreSQL
        // -------------------------------------------------------------
        const existingOrderRes = await db.query(
          "SELECT id, customer, product, quantity, status FROM orders WHERE id = $1",
          [orderId]
        );

        if (existingOrderRes.rows.length === 0) {
          console.warn(`[WARNING] Order ${orderId} not found in database.`);
        } else {
          const currentStatus = existingOrderRes.rows[0].status;

          // If already completed, skip duplicate processing (Idempotent safe-guard)
          if (currentStatus === "COMPLETED") {
            console.log(
              `[IDEMPOTENT SKIP] Order ${orderId} is already COMPLETED. Acknowledging redelivered message without re-processing.`
            );
            channel.ack(message);
            return;
          }

          if (currentStatus === "FAILED") {
            console.log(`[SKIP] Order ${orderId} is marked as FAILED. Acknowledging message.`);
            channel.ack(message);
            return;
          }
        }

        // -------------------------------------------------------------
        // Step 3: Transition to PROCESSING
        // -------------------------------------------------------------
        await db.query(
          "UPDATE orders SET status = 'PROCESSING', updated_at = NOW() WHERE id = $1 AND status = 'PLACED'",
          [orderId]
        );

        console.log(`Processing order business logic:`);
        console.log(`  Customer: ${order.customer}`);
        console.log(`  Product : ${order.product}`);
        console.log(`  Quantity: ${order.quantity}`);

        // Simulate external payment / inventory allocation work (1.5 seconds)
        await new Promise((resolve) => setTimeout(resolve, 1500));

        // -------------------------------------------------------------
        // Step 4: Transition to COMPLETED
        // -------------------------------------------------------------
        await db.query(
          "UPDATE orders SET status = 'COMPLETED', updated_at = NOW() WHERE id = $1",
          [orderId]
        );

        console.log(`[SUCCESS] Order ${orderId} marked COMPLETED in database.`);

        // -------------------------------------------------------------
        // Step 5: Acknowledge RabbitMQ message
        // Crucial: Only ACK after DB write has succeeded!
        // -------------------------------------------------------------
        channel.ack(message);
        console.log(`[ACK] Message acknowledged to RabbitMQ for ${orderId}`);
      } catch (processingError) {
        console.error(`[ERROR] Processing failed for order ${orderId}:`, processingError.message);

        // -------------------------------------------------------------
        // Step 6: Safe Bounded Retry & DLQ Routing
        // Avoid infinite requeue loops
        // -------------------------------------------------------------
        if (retryCount >= MAX_RETRIES) {
          console.error(
            `[MAX RETRIES REACHED] Order ${orderId} failed ${retryCount} times. Moving to ${DLQ}.`
          );

          try {
            // Update order status in DB to FAILED
            await db.query(
              "UPDATE orders SET status = 'FAILED', updated_at = NOW() WHERE id = $1",
              [orderId]
            );

            // Publish message to Dead Letter Queue
            channel.sendToQueue(DLQ, message.content, {
              persistent: true,
              headers: {
                "x-retry-count": retryCount,
                "x-error-message": processingError.message,
                "x-failed-at": new Date().toISOString(),
              },
            });

            // Ack from primary queue so it stops blocking
            channel.ack(message);
          } catch (dlqErr) {
            console.error("Critical error while routing to DLQ:", dlqErr);
            // If DB/RabbitMQ is completely unreachable, reject without requeue
            channel.nack(message, false, false);
          }
        } else {
          console.log(`[RETRY] Requeuing order ${orderId} for retry (attempt ${retryCount + 1})...`);
          // Transient failure: requeue once for another attempt
          channel.nack(message, false, true);
        }
      }
    },
    { noAck: false } // Explicit ACKs required for reliability
  );

  // Graceful shutdown
  const shutdown = async () => {
    console.log("\nGracefully shutting down consumer...");
    if (channel) await channel.close();
    if (connection) await connection.close();
    await db.pool.end();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

startConsumer().catch((error) => {
  console.error("Consumer startup failed:", error);
  process.exit(1);
});
