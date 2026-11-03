# 🏛️ Learning System Design: Practical & Hands-On

A comprehensive, hands-on repository dedicated to learning and mastering **System Design and Distributed Systems** through practical code, real-world patterns, and runnable lab environments.

---

## 🗺️ System Design Learning Roadmap

| Module | Topic | Pattern / Technology | Directory | Status |
| :--- | :--- | :--- | :--- | :--- |
| **01** | **Distributed Message Streaming** | Apache Kafka (Partitions, Consumer Groups, Offsets) | [`kafka-lab/`](./kafka-lab) | ✅ Completed |
| **02** | **Reliable Message Queuing & Outbox** | RabbitMQ + PostgreSQL (Transactional Outbox, DLQ) | [`rabbitmq-demo/`](./rabbitmq-demo) | ✅ Completed |
| **03** | **Distributed Caching & In-Memory Store** | Redis (Cache-Aside, Write-Through, Invalidation) | *`redis-cache/`* | ⏳ Coming Soon |
| **04** | **API Rate Limiting & Throttling** | Token Bucket / Leaky Bucket Algorithms | *`rate-limiter/`* | ⏳ Coming Soon |
| **05** | **Load Balancing & Reverse Proxies** | NGINX / Round Robin & Least Connections | *`load-balancer/`* | ⏳ Coming Soon |
| **06** | **Database Scaling & Partitioning** | Sharding, Read-Replicas & Replication Lag | *`db-sharding/`* | ⏳ Coming Soon |

---

## ⚖️ System Design Focus: Kafka vs. RabbitMQ

A classic architectural dilemma in System Design: when should you use **Kafka** vs. **RabbitMQ**? This repository includes hands-on labs for both:

| Feature | Apache Kafka (`kafka-lab/`) | RabbitMQ (`rabbitmq-demo/`) |
| :--- | :--- | :--- |
| **Model** | Distributed Append-Only Log (Pull-based) | Smart Broker / Message Queue (Push-based) |
| **Best For** | High-throughput event streaming, metrics, event sourcing | Complex routing, task queuing, request/reply, transactional workflows |
| **Message Consumption** | Consumers track offsets; messages persist even after consumption | Messages are deleted from queue once acknowledged (ACK) |
| **Ordering** | Guaranteed per-partition using message keys | FIFO within a single queue |
| **Scaling** | Partition-based consumer groups | Multiple worker threads / prefetch consumer scaling |
| **Failure Handling** | Reprocessing via offset rewind; consumer lag monitoring | Dead-Letter Queues (DLQ), poison pill routing, retries |

---

## 📂 Repository Structure

```text
Learning-System-Desgin/
│
├── .gitignore                      # Global gitignore (node_modules, logs, .env)
├── README.md                       # Main System Design roadmap & documentation
│
├── kafka-lab/                      # Module 01: Apache Kafka
│   ├── docker-compose.yml          # Kafka Broker in KRaft mode (no Zookeeper)
│   ├── package.json                # Dependencies and npm scripts
│   ├── producer.js                 # Produces events with key-based partitioning
│   ├── consumer.js                 # Primary consumer instance
│   └── consumer2.js                # Second consumer demonstrating group rebalancing
│
└── rabbitmq-demo/                  # Module 02: RabbitMQ & Transactional Outbox
    ├── docker-compose.yml          # PostgreSQL 16 + RabbitMQ (Management UI)
    ├── init.sql                    # Orders & Outbox schema with indexes
    ├── .env.example                # Environment variables template
    ├── server.js                   # Express REST API & Web Dashboard
    ├── outbox_publisher.js         # Transactional Outbox poller (FOR UPDATE SKIP LOCKED)
    ├── consumer.js                 # Resilient consumer (DLQ, prefetch, poison pill safe)
    ├── producer.js                 # Test message publisher
    ├── db.js                       # PostgreSQL connection pooling
    ├── run_tests.ps1               # Automated test runner
    ├── package.json                # Dependencies and npm scripts
    ├── README.md                   # Detailed Outbox pattern deep-dive
    └── public/
        └── index.html              # Real-time Web Dashboard UI
```

---

## 🚀 Getting Started

Each module is **completely self-contained** with its own `docker-compose.yml` and dependencies. You can dive into any module directly:

### 1. Run Module 01: Apache Kafka Lab
```bash
cd kafka-lab
docker compose up -d
npm install
npm run start:consumer   # In Terminal 1
npm run start:producer   # In Terminal 2
```
👉 *Read the full guide in [kafka-lab/README.md](./README.md#module-01-apache-kafka--event-driven-architecture).*

### 2. Run Module 02: RabbitMQ & Transactional Outbox Lab
```bash
cd rabbitmq-demo
docker compose up -d
npm install
cp .env.example .env
npm run start:server     # Terminal 1: API & Dashboard on http://localhost:3000
npm run start:publisher  # Terminal 2: Outbox Publisher
npm run start:consumer   # Terminal 3: Resilient Worker with DLQ
```
👉 *Read the full architectural guide in [rabbitmq-demo/README.md](./rabbitmq-demo/README.md).*

---

## 🤝 Contributing & Learning
This repository is an ongoing hands-on journey through System Design and Distributed Systems. Star ⭐ the repository and watch for new modules!
