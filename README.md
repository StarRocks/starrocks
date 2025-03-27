# Welcome to Stella
Stella is a commercial multi-modal database differentiated from the open-source StarRocks. We guarantee API-level compatibility with the open-source StarRocks for users. Through the reimplementation of multiple commercial-grade features, Stella delivers a multi-modal analytics engine that achieves comprehensive leadership over the open-source StarRocks in both performance and stability.

The nomenclature "Stella" carries three layers of technical semantics:

* Astrological Inheritance: Rooted in Latin for "star", it signifies the evolutionary relationship with StarRocks while demonstrating architectural advancements.
* Acronym Philosophy: As the abbreviation for StarRocks Turbocharged Efficient & Lightning-fast Lakehouse Architecture, it encapsulates our core technical proposition.
* Lakehouse-Native Identity: The celestial metaphor aligns with modern data infrastructure paradigms, positioning Stella as the gravitational center for analytical and AI workloads.

This multidimensional naming strategy particularly reflects our vision to deliver stellar performance while maintaining backward compatibility with StarRocks ecosystems.

## Features

* **Self-developed, full-featured lakehouse engine optimized for open formats, delivering data-driven materialized views and AI-native analytics:**
    + Unified Multimodal Data Access Layer: Stella introduces a converged data access layer hat supports multimodal data analysis.
    + Data-Driven Materialized Views: Auto-optimized through query pattern analysis and cost-based optimization (CBO)
    + Adaptive Storage Tiering: Stella implements deep format-specific optimizations for Parquet and ORC through encoding optimization and I/O acceleration
* **Self-developed storage engine with adaptive format optimization:**
    + Self-Optimizing File Formats: Automatic selection between different formats based on workload patterns, achieving higher compression efficiency improvements over static formats.
    + Dynamic Indexing Architecture: Hybrid LSM index, adapting to query characteristics at runtime, supporting cloud-native inverted index and high-performance json processor.
    + AI-Native Storage Tiering: Intelligent data organization optimized for both analytical and vector search workloads through adaptive segment clustering.
* **Deeply optimized computation engine:**
    + HBO-Driven Operator Adaptation: Implements fully autonomous operator selection (join/aggregation/sort) based on real-time resource metrics and historical query patterns, achieving 5-8× acceleration for complex OLAP workloads.
    + Cost-Based Materialized View Rewriting: Automatically generates and optimizes materialized views via query pattern analysis and cost modeling.

## Benefits
Stella delivers optimized performance for mission-critical enterprise scenarios:

* **Large-Scale Analytics**

Enables interactive analysis on PB-scale datasets with distributed vectorized execution engine, delivering 40% higher TPC-DS throughput versus OSS baseline

* **High-Velocity ETL**

Proprietary storage engine supports million-level TPS ingestion with adaptive compression algorithms reducing storage footprint by 30%

* **Log Intelligence**

Hybrid LSM indexing achieves millisecond search latency, combined with inverted indexes for multidimensional filtering

* **Vector Search**

AI-Native storage with integrated Faiss/HNSW indexes enables sub-millisecond retrieval across billion-scale vectors

* **Lakehouse Analytics**

Lake-oriented engine provides direct query on Paimon/Iceberg with smart MVs accelerating performance 5-8×