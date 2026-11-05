# 🔌 Module 03: Distributed Resilience & Circuit Breaker Pattern

A hands-on System Design lab demonstrating the **Circuit Breaker Pattern** for microservice resilience, cascading failure prevention, and graceful degradation using **Node.js, Express, Redis, and PostgreSQL**.

---

## 🎯 Overview & Objectives

In distributed architectures, services communicate over networks that are inherently unreliable. When an upstream service (e.g., Profile Service or Payment Gateway) degrades or fails:
1. Callers wait and consume threads/sockets, causing **cascading resource exhaustion**.
2. Retrying blindly can trigger a **thundering herd** and prevent the failing service from recovering.
3. The **Circuit Breaker Pattern** wraps calls and tracks failure counts. When failures exceed a threshold, the breaker trips to **OPEN**, immediately failing fast or returning cached fallback responses without hitting the degraded dependency.

---

## 🧩 Architectural States

```text
       [Normal Operations]
       ┌─────────────────┐
  ┌───>│     CLOSED      │<────────────────────────┐
  │    │ (Requests Pass) │                         │
  │    └────────┬────────┘                         │
  │             │ Failures > Threshold             │ Successes > Threshold
  │             ▼                                  │
  │    ┌─────────────────┐                ┌────────┴────────┐
  │    │      OPEN       │─[Sleep Window]─>   HALF-OPEN     │
  │    │  (Fail Fast /   │    Expires     │ (Trial Request) │
  │    │ Cached Fallback)│                └────────┬────────┘
  │    └─────────────────┘                         │
  │                                                │ Failed Trial
  └────────────────────────────────────────────────┘
```

* **CLOSED**: Requests flow through to the service normally. Failures increment an error counter.
* **OPEN**: The service is failing. Requests immediately fail fast or return fallback data from cache without calling the upstream service.
* **HALF-OPEN**: After a cooling sleep window, a trial request is allowed through. If it succeeds, the breaker resets to `CLOSED`. If it fails, the breaker trips back to `OPEN`.

---

## 📂 Lab Structure

```text
circuit-breaker-lab/
├── .env.example        # Environment variables template
├── db.js               # PostgreSQL connection pool
├── init.sql            # Database schema for persistent circuit breaker state
├── package.json        # Dependencies and npm scripts
├── profile-service.js  # Upstream Profile Service with simulated failure switch
├── redis.js            # Redis client for caching and fast distributed state
└── test-db.js          # PostgreSQL connection verification script
```

---

## 🚀 Getting Started

### 1. Environment Setup
Copy the template and configure your local PostgreSQL credentials:
```bash
cp .env.example .env
```

### 2. Install Dependencies
```bash
npm install
```

### 3. Initialize Database
Execute `init.sql` against your PostgreSQL instance to create the `circuit_breakers` tracking table:
```bash
psql -h localhost -p 5433 -U postgres -d circuit_breaker_db -f init.sql
```

### 4. Verify Database Connection
```bash
node test-db.js
```

### 5. Run Profile Service
```bash
node profile-service.js
```
* **Fetch Profile**: `GET http://localhost:4000/profile/:userId`
* **Trigger Simulated Failure Mode**:
  ```bash
  curl -X POST http://localhost:4000/admin/failure-mode \
    -H "Content-Type: application/json" \
    -d '{"shouldFail": true}'
  ```
