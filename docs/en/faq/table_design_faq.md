---
displayed_sidebar: docs
---

## Troubleshooting Table Design

This topic provides answers to some frequently asked questions about table design.

## What is the maximum length of the VARCHAR type and how does the length affects query performance?

The maximum length of the VARCHAR type is 65533, which requires 1 MB storage size. It is recommended to set the VARCHAR length to a minimum value that is necessary. It is because, despite that the VARCHAR type data size is based on the actual length, in query scenarios where memory pre-allocation is required, memory resources are allocated based on the pre-defined length of the VARCHAR types instead of the actual length. For example, for an `address` field, 100 bytes is enough, which means that VARCHAR(100) is recommended over STRING, because the STRING type is equivalent to VARCHAR(65533). 
