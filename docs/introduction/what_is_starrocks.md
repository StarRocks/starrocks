# What is StarRocks?

StarRocks is a next-generation, blazing-fast massively parallel processing (MPP) database designed to make real-time analytics easy for enterprises. It is built to power sub-second queries at scale.

StarRocks has an elegant design. It encompasses a rich set of features including fully vectorized engine, newly designed cost-based optimizer (CBO), and intelligent materialized view. As such, StarRocks can deliver a query speed far exceeding database products of its kind, especially for multi-table joins.

StarRocks is ideal for real-time analytics on fresh data. Data can be ingested at a high speed and updated and deleted in real time. StarRocks empowers users to create tables that use various schemas, such as flat, star, and snowflake schemas.

Compatible with MySQL protocols and standard SQL, StarRocks has out-of-the-box support for all major BI tools, such as Tableau and Power BI. StarRocks does not rely on any external components. It is an integrated data analytics platform that allows for high scalability, high availability, and simplified management and maintenance.

## Scenarios

StarRocks meets varied enterprise analytics requirements, including OLAP multi-dimensional analytics, real-time analytics, high-concurrency analytics, customized reporting, ad-hoc queries, and unified analytics.

### OLAP multi-dimensional analytics

The MPP framework and vectorized execution engine enable users to choose between various schemas to develop multi-dimensional analytical reports. Scenarios:

- User behavior analysis

- User profiling, label analysis, user tagging

- High-dimensional metrics report

- Self-service dashboard

- Service anomaly probing and analysis

- Cross-theme analysis

- Financial data analysis

- System monitoring analysis

### Real-time analytics

StarRocks uses the Primary Key model to implement real-time updates. Data changes in a TP database can be synchronized to StarRocks in a matter of seconds to build a real-time warehouse.

Scenarios:

- Online promotion analysis

- Logistics tracking and analysis

- Performance analysis and metrics computation for the financial industry

- Quality analysis for livestreaming

- Ad placement analysis

- Cockpit management

- Application Performance Management (APM)

### High-concurrency analytics

StarRocks leverages performant data distribution, flexible indexing, and intelligent materialized views to facilitate user-facing analytics at high concurrency:

- Advertiser report analysis

- Channel analysis for the retail industry

- User-facing analysis for SaaS

- Multi-tabbed dashboard analysis

### Unified analytics

StarRocks provides a unified data analytics experience.

- One system can power various analytical scenarios, reducing system complexity and lowering TCO.

- StarRocks unifies data lakes and data warehouses. Data in a lakehouse can be managed all in StarRocks. Latency-sensitive queries that require high concurrency can run on StarRocks. Data in data lakes can be accessed by using external catalogs or external tables provided by StarRocks.
