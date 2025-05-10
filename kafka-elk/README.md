# 🧪 Kafka Lab with Docker Compose

This project sets up a local Apache Kafka environment for experimentation using Docker Compose. It includes:

- Apache Kafka broker
- Kafka UI (Kafka-UI)
- A CLI container (kcat) to test topics
- A Python-based producer and consumer
- Automatic topic creation and data streaming

---

## 🚀 Getting Started

### Start all containers

```bash
docker compose up --build -d
```

Here’s a **basic README** for your Docker Compose Kafka lab project. It includes:

* How to run the stack
* How to reset all resources
* URLs to access Kafka UI
* Notes on using `kcat`, producer, and consumer

---

### 📘 README.md

````markdown
# 🧪 Kafka Lab with Docker Compose

This project sets up a local Apache Kafka environment for experimentation using Docker Compose. It includes:

- Apache Kafka broker
- Kafka UI (Kafka-UI)
- A CLI container (kcat) to test topics
- A Python-based producer and consumer
- Automatic topic creation and data streaming

---

## 🚀 Getting Started

### Start all containers

```bash
docker compose up --build -d
````

This will spin up:

* 🟢 Kafka broker
* 🟢 Kafka UI (web interface)
* 🟢 kcat CLI (for Kafka command line testing)
* 🟢 Custom producer and consumer (Python)

---

## 🔁 Reset the environment

If you want to completely reset all Kafka data and containers:

```bash
docker compose down -v
docker compose up --build -d
```

This deletes **volumes** (Kafka persisted data) and recreates containers from scratch.

---

## 🔗 Accessing Tools

| Tool                 | URL/Command                                    | Notes                             |
| -------------------- | ---------------------------------------------- | --------------------------------- |
| **Kafka UI**         | [http://localhost:8080](http://localhost:8080) | Inspect topics, messages, offsets |
| **Kafka CLI** (kcat) | `docker exec -it kcat bash`                    | Run `kcat` inside CLI container   |
| **Producer**         | Custom Python script in `./producer`           | Sends fake payment JSON to Kafka  |
| **Consumer**         | Custom Python script in `./consumer`           | Prints Kafka messages to console  |

---

## 📂 Project Structure

```text
.
├── docker-compose.yml
├── producer/         # Fake data producer (Python + Faker)
├── consumer/         # Kafka consumer (Python)
└── README.md
```

---

## 📝 Notes

* Default topic: `payments`
* Kafka is exposed on `kafka-node:9092` **inside the Docker network**
* You can create additional producers/consumers or plug in Elasticsearch for ingestion
