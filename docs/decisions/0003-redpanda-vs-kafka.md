# ADR-0003: Redpanda instead of Apache Kafka + Zookeeper

- **Status:** Accepted
- **Date:** 2026-09-25

## Context

The spec says "Apache Kafka." Classic Kafka in Docker means:
- A **Zookeeper** container (older setups) or KRaft-mode config.
- A JVM-based broker that is slow to start and heavy on memory.
- Fiddly `advertised.listeners` configuration to make it reachable from both inside the
  Docker network and from the host.

For a laptop dev loop this is a lot of friction before you write a single line of pipeline
code.

## Decision

Use **Redpanda** as the message broker. Redpanda:
- Speaks the **Kafka wire protocol** — our Go client (`segmentio/kafka-go`) and every Kafka
  concept (topics, partitions, consumer groups, offsets) work unchanged.
- Is a **single C++ binary**, no JVM, no Zookeeper. Starts in ~1 second.
- Uses far less memory, which matters when TimescaleDB, Redis, Prometheus, Grafana and four
  Go services also run on the same laptop.
- Ships `rpk`, a friendly CLI for creating topics and inspecting lag.

## Consequences

- **Nothing in the application code is Redpanda-specific.** Point the client at a real Kafka
  cluster (change one broker address) and it works. The learning transfers 1:1.
- Simpler `docker-compose.yml`, faster `make up`.
- **Trade-off:** In a real job you'll likely meet managed Kafka (MSK, Confluent). The
  operational differences (partition reassignment tooling, ACLs, tiered storage) aren't
  exercised here — but the *client-side* and *conceptual* knowledge is identical.

## Note

Everywhere in the docs we still say "Kafka" when talking about concepts (topics, partitions,
consumer groups), because those are Kafka-protocol concepts that Redpanda implements. We say
"Redpanda" only when talking about the specific broker we run.
