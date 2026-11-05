
const pool = require("./db");

async function testDatabase() {
    try {
        const result = await pool.query(
            "SELECT * FROM circuit_breakers"
        );

        console.log("Database connected successfully!");
        console.log("Circuit Breaker Records:", result.rows);

    } catch (error) {
        console.error("Database connection failed:", error);
    } finally {
        await pool.end();
    }
}

testDatabase();
