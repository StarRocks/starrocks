---
displayed_sidebar: docs
---

# Introduction to Query Tuning and Plans in StarRocks

Query tuning is essential for achieving high performance and reliability in StarRocks. This directory brings together practical guides, reference materials, and actionable recipes to help you analyze, diagnose, and optimize query performance at every stage—from writing SQL to interpreting execution details.

Below you'll find a brief overview of each document in this section, with links to help you quickly find the right resource for your needs:

- **[Query Plan Overview](./query_planning.md)**  
  Learn the fundamentals of how StarRocks plans and executes queries. Covers the structure of query plans, how to read them, and how to use execution profiles for diagnosis.

- **[Query Profile Overview](./query_profile_overview.md)**  
  An introduction to the Query Profile feature, including how to enable, access, and interpret query execution profiles in StarRocks.
  
- **[Query Profile Tuning Recipes](./query_profile_tuning_recipes.md)**  
  Symptom-driven guides for diagnosing and fixing common query performance issues. Move from red-flag metrics to root cause and proven solutions.

- **[Performance Optimization Tips](./performance_opt_tips.md)**  
  Practical advice for schema design, table types, and other foundational choices that impact query performance.

- **[Explain Analyze & Text-Based Profile Analysis](./query_profile_text_based_analysis.md)**  
  Step-by-step instructions for simulating and analyzing query execution using EXPLAIN ANALYZE and text-based profiles.

- **[Query Hint](./query_hint.md)**  
  Learn how to use hints to guide the optimizer, control join strategies, and set execution variables for special cases.

- **[Query Profile Metrics](./query_profile_operator_metrics.md)**  
  A comprehensive reference for all metrics and operators found in query profiles. Use this as a glossary when troubleshooting or tuning.

Whether you're a DBA, developer, or data engineer, these resources will help you:
- Diagnose and resolve slow or resource-intensive queries
- Understand optimizer choices and execution details
- Apply best practices and advanced tuning strategies

Start with the overview, dive into the references as needed, and use the recipes and tips to solve real-world performance challenges in StarRocks. 