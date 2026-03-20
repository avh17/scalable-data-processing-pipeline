A real-time, highly available data processing pipeline built on Kubernetes that streams document data through Kafka into a Neo4j graph database for near real-time graph analytics.

## Architecture

<img width="1672" height="878" alt="image" src="https://github.com/user-attachments/assets/ab4bcd99-37e3-4e22-8cd4-7042b6daf98d" />

## Overview

The pipeline orchestrates **4 containerized services** on Kubernetes:

| Service | Type | Role |
|---|---|---|
| **Kafka** | Deployment | Distributed message broker for ingesting document streams |
| **Zookeeper** | Deployment | Coordination service for Kafka cluster management |
| **Neo4j** | StatefulSet (Helm) | Graph database for storing and querying relationship data |
| **Kafka Connect** | Deployment | Bridges Kafka topics to Neo4j via the Neo4j Connector |

## Graph Algorithms

### PageRank

Computes the relative importance of each node in the document graph using the Neo4j GDS library. Identifies the most influential documents/entities based on their connections.

### Breadth-First Search (BFS)

Traverses the graph from a source node, exploring all neighbors at each depth level before moving deeper. Used for shortest-path discovery and connectivity analysis across the document graph.

## Key Design Decisions

- **Kafka as the ingestion layer** decouples producers from consumers, enabling independent scaling and replay of document streams
- **Kafka Connect with the Neo4j Sink Connector** eliminates custom ETL code by declaratively mapping Kafka topics to Neo4j Cypher statements
- **Neo4j as a StatefulSet** ensures data persistence across pod restarts, critical for a database workload
- **Load-balanced ingress/egress** allows external producers and analytics clients to interact with the pipeline without direct pod access
