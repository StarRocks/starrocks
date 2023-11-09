---
displayed_sidebar: "English"
---

### Advantages of Broker Load

- Broker Load supports data transformation, UPSERT, and DELETE operations during loading.
- Broker Load runs in the background and clients don't need to stay connected for the job to continue.
- Broker Load is preferred for long running jobs, the default timeout is 4 hours.
- Broker Load supports Parquet, ORC, and CSV file format.

### Data flow

![Workflow of Broker Load](../broker_load_how-to-work_en.png)

1. The user creates a load job
2. The frontend (FE) creates a query plan and distributes the plan to the backend nodes (BE)
3. The backend (BE) nodes pull the data from the source and load the data into StarRocks
