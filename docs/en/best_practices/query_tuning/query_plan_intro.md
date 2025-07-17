---
displayed_sidebar: docs
sidebar_position: 10
---

# Introduction to Query Tuning

Query tuning is essential for achieving high performance and reliability in StarRocks. This directory brings together practical guides, reference materials, and actionable recipes to help you analyze, diagnose, and optimize query performance at every stage—from writing SQL to interpreting execution details.


Effective query tuning in StarRocks typically follows a top-down process:

1. **Identify the Problem**  
   - Detect slow queries, high resource usage, or unexpected results.  
   - In StarRocks, leverage built-in monitoring tools, query history, and audit logs to quickly identify problematic queries or unusual patterns.  
   - See: **[Query Tuning Recipes](./query_profile_tuning_recipes.md)** for symptom-driven diagnosis and **[Query Profile Overview](./query_profile_overview.md)** for accessing query history and profiles.

2. **Collect and Analyze Execution Information**  
   - Obtain the query plan using `EXPLAIN` or `EXPLAIN ANALYZE`.  
   - Enable and review the Query Profile to gather detailed execution metrics.  
   - See: **[Query Plan Overview](./query_planning.md)** for understanding query plans, **[Explain Analyze & Text-Based Profile Analysis](./query_profile_text_based_analysis.md)** for step-by-step analysis, and **[Query Profile Overview](./query_profile_overview.md)** for enabling and interpreting profiles.

3. **Locate the Root Cause**  
   - Pinpoint which stage or operator is consuming the most time or resources.  
   - Check for common issues: suboptimal join order, missing indexes, data distribution problems, or inefficient SQL patterns.  
   - See: **[Query Profile Metrics](./query_profile_operator_metrics.md)** for a glossary of metrics and operators, and **[Query Tuning Recipes](./query_profile_tuning_recipes.md)** for root cause analysis.

4. **Apply Tuning Strategies**  
   - SQL Rewrite: rewrite or optimize the SQL query (e.g., add filters, avoid SELECT *).  
   - Schema tuning: add indexes, change table types, partitioning, clustering.  
   - Query plan tuning: use hints or variables to guide the optimizer if necessary.  
   - Execution tuning: tune session variables for specific workloads.  
   - See: **[Schema Tuning Recipes](./schema_tuning.md)** for schema-level optimizations, **[Query Hint](./query_hint.md)** for optimizer hints, and **[Query Tuning Recipes](./query_profile_tuning_recipes.md)** for plan tuning and execution tuning.

5. **Validate and Iterate**  
   - Rerun the query and compare performance before and after changes.  
   - Review the new query plan and profile to ensure improvements.  
   - Repeat the process as needed for further optimization.  


Whether you're a DBA, developer, or data engineer, these resources will help you:
- Diagnose and resolve slow or resource-intensive queries
- Understand optimizer choices and execution details
- Apply best practices and advanced tuning strategies

Start with the overview, dive into the references as needed, and use the recipes and tips to solve real-world performance challenges in StarRocks. 
