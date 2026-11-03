const { Pool } = require("pg");
require("dotenv").config();

const pool = new Pool({
  host: process.env.PGHOST || "localhost",
  port: parseInt(process.env.PGPORT || "5432", 10),
  user: process.env.PGUSER || "postgres",
  password: process.env.PGPASSWORD || "postgres",
  database: process.env.PGDATABASE || "orders_db",
  max: 10,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

pool.on("error", (err) => {
  console.error("Unexpected error on idle PostgreSQL client:", err);
});

async function query(text, params) {
  const start = Date.now();
  const res = await pool.query(text, params);
  const duration = Date.now() - start;
  // console.debug("Executed query", { text, duration, rows: res.rowCount });
  return res;
}

async function testConnection() {
  const client = await pool.connect();
  try {
    const res = await client.query("SELECT NOW()");
    console.log("Connected to PostgreSQL successfully at:", res.rows[0].now);
  } finally {
    client.release();
  }
}

module.exports = {
  pool,
  query,
  testConnection,
};
