require("dotenv").config();
const express = require("express");
const path = require("path");
const { randomUUID } = require("crypto");
const db = require("./db");

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, "public")));

const PORT = process.env.PORT || 3000;

async function startServer() {
  // 1. Verify PostgreSQL connection
  await db.testConnection();

  console.log(`\n======================================================`);
  console.log(` Order API Server (Transactional Outbox Pattern)`);
  console.log(` Web Demo Dashboard: http://localhost:${PORT}`);
  console.log(`======================================================\n`);

  // -------------------------------------------------------------
  // POST /orders - Atomic Transaction: Order + Outbox Event
  // -------------------------------------------------------------
  app.post("/orders", async (req, res) => {
    const { customer, product, quantity } = req.body;

    if (
      typeof customer !== "string" ||
      !customer.trim() ||
      typeof product !== "string" ||
      !product.trim() ||
      !Number.isInteger(quantity) ||
      quantity < 1
    ) {
      return res.status(400).json({
        error: "Provide customer, product, and a positive integer quantity",
      });
    }

    const orderId = randomUUID();
    const outboxId = randomUUID();

    const client = await db.pool.connect();
    try {
      // ATOMIC TRANSACTION: Both order and outbox event are saved together!
      await client.query("BEGIN");

      // 1. Insert order record
      const orderRes = await client.query(
        `INSERT INTO orders (id, customer, product, quantity, status)
         VALUES ($1, $2, $3, $4, 'PLACED')
         RETURNING *;`,
        [orderId, customer.trim(), product.trim(), quantity]
      );
      const order = orderRes.rows[0];

      // 2. Insert outbox event record
      const outboxRes = await client.query(
        `INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, status)
         VALUES ($1, 'ORDER', $2, 'ORDER_CREATED', $3, 'PENDING')
         RETURNING *;`,
        [outboxId, orderId, JSON.stringify(order)]
      );
      const outboxEvent = outboxRes.rows[0];

      await client.query("COMMIT");

      console.log(`[ATOMIC TX SUCCESS] Order ${orderId} + Outbox Event ${outboxId} committed.`);

      return res.status(202).json({
        message: "Order placed and outbox event recorded atomically in PostgreSQL.",
        order,
        outboxEvent,
      });
    } catch (error) {
      await client.query("ROLLBACK");
      console.error("[ATOMIC TX ERROR] Order creation failed:", error.message);

      return res.status(503).json({
        error: "Unable to commit order and outbox event to database.",
        details: error.message,
      });
    } finally {
      client.release();
    }
  });

  // -------------------------------------------------------------
  // GET /orders - List recent orders
  // -------------------------------------------------------------
  app.get("/orders", async (req, res) => {
    try {
      const result = await db.query(
        "SELECT * FROM orders ORDER BY created_at DESC LIMIT 50"
      );
      return res.status(200).json({
        count: result.rows.length,
        orders: result.rows,
      });
    } catch (error) {
      console.error("Error fetching orders:", error.message);
      return res.status(500).json({ error: "Internal server error" });
    }
  });

  // -------------------------------------------------------------
  // GET /orders/:id - Fetch single order
  // -------------------------------------------------------------
  app.get("/orders/:id", async (req, res) => {
    const { id } = req.params;
    try {
      const result = await db.query("SELECT * FROM orders WHERE id = $1", [id]);
      if (result.rows.length === 0) {
        return res.status(404).json({ error: "Order not found" });
      }
      return res.status(200).json({ order: result.rows[0] });
    } catch (error) {
      console.error("Error fetching order by ID:", error.message);
      return res.status(500).json({ error: "Internal server error" });
    }
  });

  // -------------------------------------------------------------
  // GET /outbox - List recent outbox events
  // -------------------------------------------------------------
  app.get("/outbox", async (req, res) => {
    try {
      const result = await db.query(
        "SELECT * FROM outbox_events ORDER BY created_at DESC LIMIT 50"
      );
      return res.status(200).json({
        count: result.rows.length,
        events: result.rows,
      });
    } catch (error) {
      console.error("Error fetching outbox events:", error.message);
      return res.status(500).json({ error: "Internal server error" });
    }
  });

  // -------------------------------------------------------------
  // GET /outbox/:id - Fetch single outbox event
  // -------------------------------------------------------------
  app.get("/outbox/:id", async (req, res) => {
    const { id } = req.params;
    try {
      const result = await db.query("SELECT * FROM outbox_events WHERE id = $1", [id]);
      if (result.rows.length === 0) {
        return res.status(404).json({ error: "Outbox event not found" });
      }
      return res.status(200).json({ event: result.rows[0] });
    } catch (error) {
      console.error("Error fetching outbox event by ID:", error.message);
      return res.status(500).json({ error: "Internal server error" });
    }
  });

  const server = app.listen(PORT, () => {
    console.log(`Server listening on port ${PORT}`);
  });

  const shutdown = async () => {
    console.log("\nGracefully shutting down Order API...");
    server.close();
    await db.pool.end();
    process.exit(0);
  };

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

startServer().catch((error) => {
  console.error("Server startup failed:", error);
  process.exit(1);
});
