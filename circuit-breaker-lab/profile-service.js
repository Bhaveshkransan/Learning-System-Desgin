
const express = require("express");

const app = express();

app.use(express.json());

// Simulates whether Profile Service is healthy
let shouldFail = false;

// GET user profile
app.get("/profile/:userId", (req, res) => {

    if (shouldFail) {
        console.log("Profile Service is failing!");

        return res.status(503).json({
            success: false,
            message: "Profile Service is temporarily unavailable"
        });
    }

    const userId = req.params.userId;

    console.log(`Serving profile for ${userId}`);

    return res.status(200).json({
        success: true,
        profile: {
            userId,
            name: "Test User",
            city: "Mumbai"
        }
    });
});

// Enable or disable simulated failure
app.post("/admin/failure-mode", (req, res) => {

    shouldFail = req.body.shouldFail;

    return res.json({
        message: shouldFail
            ? "Profile Service failure enabled"
            : "Profile Service recovered",
        shouldFail
    });
});

const PORT = 4000;

app.listen(PORT, () => {
    console.log(`Profile Service running on port ${PORT}`);
});
