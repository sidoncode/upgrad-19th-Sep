# upgrad-19th-Sep

# Apache Kafka : Real-Time Streaming with GitHub Codespaces and Docker

**Use case:** An online store emits an event every time a customer places an order. We'll stream these order events through Kafka and read them in real time, like a fulfilment or analytics service would.

**You need:** a GitHub account. Nothing to install locally.

---

## Section 1: Setup and Context

### 1.1 Why real-time streaming?

Traditional pipelines are **batch**: collect data all day, process it at night. That is too slow when:

- a fraud check must happen *before* the payment completes
- a dashboard should show today's sales *right now*
- a warehouse needs to start packing the moment an order lands

**Streaming** treats data as a continuous flow of events that can be processed as they arrive.

### 1.2 Where Kafka fits

Kafka is a distributed, durable **event log** that sits between systems that produce data and systems that consume it.

```
 Web app ─┐                              ┌─► Fulfilment service
 Mobile ──┼──► [ KAFKA: "orders" ] ──────┼─► Analytics dashboard
 POS  ────┘                              └─► Fraud detection
   PRODUCERS         BROKER                      CONSUMERS
```

Producers and consumers never talk to each other directly. Each side can scale, fail, or be replaced independently.

### 1.3 Start the environment

**Step 1.** Create a new GitHub repository (e.g. `kafka-demo`), then click **Code → Codespaces → Create codespace on main**. Docker is preinstalled in Codespaces.

**Step 2.** In the Codespace, create a file called `docker-compose.yml`:

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    restart: on-failure
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

**Step 3 (T1 Admin).** Start the services and confirm they're running:

```bash
docker compose up -d
docker compose ps
```

You should see both `zookeeper` and `kafka` with status **running**. Verify Kafka is ready:

```bash
docker logs kafka 2>&1 | grep "started (kafka.server.KafkaServer)"
```

> **What is Zookeeper doing?** In this Kafka version, Zookeeper stores cluster metadata (which brokers exist, which topics exist). Newer Kafka releases replace it with a built-in mode called KRaft, but the concepts you learn here are identical.

> **Wait about 30 seconds** after `docker compose up -d` before running any Kafka command. If you run commands too early you'll see `container is not running` or `Broker may not be available`. See Troubleshooting at the end.

**Step 4.** Open **three terminal tabs** in Codespaces (use the **+** icon in the terminal panel, or the split icon to view them side by side). A fourth is optional for the scaling demo.

| Terminal | Role | Stays running? |
|---|---|---|
| **T1: Admin** | Setup, monitoring, cleanup | No, run a command and get the prompt back |
| **T2: Consumer** | Live consumer | Yes, it keeps waiting for messages |
| **T3: Producer** | Sends messages | Yes, it keeps the `>` prompt open |
| **T4: Consumer 2** (optional) | Second consumer for the scaling demo | Yes |

Every section below is labelled with the terminal to use, for example **(T1 Admin)**.

---

## Section 2: Kafka Core Concepts

| Concept | Meaning | Store analogy |
|---|---|---|
| **Broker** | A Kafka server that stores and serves data | The warehouse |
| **Topic** | A named stream of events | The `orders` shelf |
| **Partition** | A topic is split into ordered logs for parallelism | Aisles on the shelf |
| **Producer** | Writes events to a topic | The checkout system |
| **Consumer** | Reads events from a topic | A packing station |
| **Offset** | The position number of a message within a partition | Line number in a logbook |
| **Consumer group** | Consumers sharing a group ID split the work of a topic | A team of packers |

### Topics and partitions

```
Topic: orders
Partition 0: [0][1][2][3][4] ──►  (new messages appended)
Partition 1: [0][1][2][3]    ──►
Partition 2: [0][1][2][3][4][5] ──►
```

- Order is guaranteed **within a partition**, not across partitions.
- Messages with the same **key** (e.g. `customer_id`) always go to the same partition, so one customer's orders stay in order.

### Consumer groups and offsets

- Each partition is read by **only one consumer per group**. Add consumers to a group and Kafka spreads partitions among them (parallel processing).
- Different groups each get **their own full copy** of the stream (fulfilment and analytics both see every order).
- Kafka records each group's **committed offset**, meaning "this group has processed up to here." If a consumer restarts, it resumes from that point.

---

## Section 3: Kafka Streaming Demonstration

All commands run *inside* the Kafka container via `docker exec`.

### Run order at a glance

| Step | Terminal | Action |
|---|---|---|
| 1 | T1 Admin | Create and describe the `orders` topic |
| 2 | T2 Consumer | Start the consumer first (it will look frozen, that's normal) |
| 3 | T3 Producer | Start the producer and type orders |
| 4 | T2, T3, T1 | Crash and resume demo: `Ctrl+C` T2, send 3 orders in T3, check lag in T1, restart T2 |
| 5 | T4 (optional) | Second consumer in the same group to see partitions split |
| 6 | T1 Admin | Retention replay, config check, broker restart |
| 7 | T1 Admin | Cleanup |

Always start the consumer (T2) before the producer (T3) so you see messages arrive live.

### 3.1 Create a topic (T1 Admin)

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
  --create --topic orders \
  --partitions 3 --replication-factor 1 \
  --config retention.ms=604800000
```

Inspect it:

```bash
docker exec kafka kafka-topics --bootstrap-server kafka:9092 --describe --topic orders
```

You'll see 3 partitions, each with a **Leader** (the broker serving it), **Replicas**, and **Isr** (in-sync replicas).

### 3.2 Start a consumer (T2 Consumer)

Start this **first** so you can watch messages arrive live:

```bash
docker exec -it kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic orders --group order-processors \
  --property print.key=true --property print.partition=true
```

It will sit and wait. That's expected.

### 3.3 Produce messages (T3 Producer)

```bash
docker exec -it kafka kafka-console-producer --bootstrap-server kafka:9092 \
  --topic orders \
  --property parse.key=true --property key.separator=:
```

At the `>` prompt, type these one at a time (format is `key:value`):

```
alice:{"order_id":1,"item":"laptop","qty":1}
bob:{"order_id":2,"item":"phone","qty":2}
alice:{"order_id":3,"item":"mouse","qty":1}
carol:{"order_id":4,"item":"monitor","qty":1}
bob:{"order_id":5,"item":"charger","qty":3}
```

Switch to **T2**. Each order appears instantly with its **partition** number. Notice that `alice`'s orders always land on the same partition. That's key-based partitioning at work.

### 3.4 Fault tolerance demo: consumer crash and resume

1. In **T2 (Consumer)**, press `Ctrl+C` to simulate a crashed service.
2. In **T3 (Producer)**, send three more orders:
   ```
   dave:{"order_id":6,"item":"keyboard","qty":1}
   alice:{"order_id":7,"item":"webcam","qty":1}
   erin:{"order_id":8,"item":"desk","qty":1}
   ```
3. In **T1 (Admin)**, check the group:
   ```bash
   docker exec kafka kafka-consumer-groups --bootstrap-server kafka:9092 --describe --group order-processors
   ```
   The `LAG` column totals **3**. The messages are safely stored and waiting.
4. In **T2**, run the **same** consumer command again (same `--group order-processors`).

It receives **only orders 6, 7, 8**. Kafka remembered the committed offsets, so nothing was lost and nothing was re-processed.

### 3.5 Scale out with a consumer group (optional)

Open **T4** and run the same consumer command with the same `--group order-processors`. Kafka rebalances, and the 3 partitions are now split between the two consumers. Produce more orders and watch the work divide. Then start a consumer with a **different** group (e.g. `--group analytics --from-beginning`): it receives *every* order from the start, independently.

### 3.6 Message retention: data is not deleted on read (T1 Admin)

Unlike a traditional queue, Kafka keeps messages after they're consumed. They are removed only when the retention period expires (we set 7 days above). Prove it by replaying everything with a brand-new group:

```bash
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic orders --group replay-demo --from-beginning \
  --property print.partition=true --timeout-ms 5000
```

All 8 orders come back. Check the retention setting:

```bash
docker exec kafka kafka-configs --bootstrap-server kafka:9092 \
  --describe --entity-type topics --entity-name orders
```

### 3.7 Fault tolerance at the broker level (T1 Admin)

Restart the broker and confirm the data survives. Stop and restart the T2/T3 commands afterwards, since they lose their connection when the broker restarts:

```bash
docker restart kafka
# wait ~30 seconds, then:
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 \
  --topic orders --group after-restart --from-beginning --timeout-ms 5000
```

Messages are persisted to disk, so all 8 are still there.

> **Note on real-world fault tolerance:** Our single broker uses `replication-factor 1`, so if the *disk* were lost, the data would be lost. Production clusters run 3+ brokers with `replication-factor 3`. Each partition is copied to three brokers, one is the leader, and if the leader dies, a follower is automatically promoted. With `min.insync.replicas=2` and `acks=all`, a message is only acknowledged once at least two copies exist.

---

## Section 4: Conclusion

### Recap

| Concept | What you saw |
|---|---|
| **Streaming need** | Orders processed as they happen, not overnight |
| **Broker and Zookeeper** | Started with Docker Compose; broker stores data, Zookeeper holds metadata |
| **Topic and partitions** | `orders` split into 3 partitions; same key goes to the same partition |
| **Producer and consumer** | Console tools sent and received events live |
| **Consumer groups and offsets** | Group tracked its position; lag revealed backlog; restart resumed without loss |
| **Retention** | Messages remained after consumption and could be replayed by new groups |
| **Fault tolerance** | Consumer crash and broker restart caused no data loss; replication extends this to disk/server failures |

### Why it matters

Kafka decouples data producers from consumers and acts as the durable, replayable backbone of real-time systems. One stream of order events can simultaneously feed fulfilment, fraud detection, dashboards, and machine-learning features, each at its own pace and each able to recover from failure.

### Next steps

- Write a producer and consumer in Python (`confluent-kafka`) or Java
- Add a stream processor (Kafka Streams, Flink, or Spark Structured Streaming)
- Try Kafka Connect to stream a database into a topic
- Explore KRaft mode (Kafka without Zookeeper)

### Clean up (T1 Admin)

```bash
docker compose down -v
```

Then stop or delete the Codespace to avoid using your free hours.

---

### Troubleshooting

| Problem | Fix |
|---|---|
| `container ... is not running` | Kafka exited. Check `docker compose ps -a` and `docker logs kafka --tail 40` |
| `Connection ... could not be established. Broker may not be available` | Broker is still starting. Wait 30 seconds and confirm `docker logs kafka` shows `started (kafka.server.KafkaServer)` |
| Kafka container keeps exiting | Zookeeper wasn't ready in time. Restart in order: `docker compose down`, `docker compose up -d zookeeper`, wait 15 seconds, `docker compose up -d kafka`, wait 30 seconds |
| Name conflict on `kafka` or `zookeeper` | Leftover containers from earlier work. Run `docker ps -a` and remove them with `docker rm -f <name>` |
| Consumer/producer stopped working after `docker restart kafka` | Press `Ctrl+C` and rerun the command in that terminal |
| `--group` shows no lag/offsets | The consumer must have consumed at least once (or be running) for the group to appear |
| `Topic already exists` | Skip creation, or delete with `kafka-topics --delete --topic orders` |
