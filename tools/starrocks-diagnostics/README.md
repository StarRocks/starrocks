# CelerData Doctor v0.1

## Overview

The **CelerData Doctor** script is a comprehensive diagnostic tool designed for StarRocks clusters. It collects critical metadata and performance indicators from StarRocks clusters, providing valuable insights for troubleshooting, optimization, and capacity planning.

### Key Features
- Collects **schema**, **partitions**, and **tablets** for all tables in all databases.
- Detects **cluster architecture** (shared-nothing, shared-data, hybrid).
- Identifies tables with potential performance issues, including:
  - Incorrect replication factors
  - Oversized partitions
  - Empty tables and partitions
  - Too many tablets
  - Severe data skew (table and partition level)
  - Abnormal replica status
- Generates a compressed `.tar.gz` file for easy sharing with support teams.
- Summarizes findings in a single `summary.txt` for rapid assessment.


## Installation

### Prerequisites
- Python 3.8+
- Required packages:

```bash
pip3 install mysql-connector-python
```

Make sure the following libraries are installed:

```python
import mysql.connector
import os
import csv
from datetime import datetime
import tarfile
import shutil
```

### Clone the Repository
```bash
git clone <REPO_URL>
cd celerdata-doctor
```


## Usage

Run the script:
```bash
./celerdata-doctor.py
```

You will be prompted for the following connection details:
- **Host** (default: `127.0.0.1`)
- **Port** (default: `9030`)
- **Username** (default: `root`)
- **Password** (required)

Example:
```plaintext
Enter StarRocks host (default: 127.0.0.1):
Enter StarRocks port (default: 9030):
Enter StarRocks username (default: root):
Enter StarRocks password:
✅ Connected to StarRocks
```


## Output Structure

After execution, the script will generate a compressed file like:
```
starrocks_metadata_<timestamp>.tar.gz
```

Extracting this archive reveals:
```
starrocks_metadata_<timestamp>/
├── default_catalog/
│   ├── <database>/
│   │   ├── tables.txt
│   │   ├── <table>.sql
│   │   ├── <table>_partitions.csv
│   │   └── <table>_tablets.csv
└── performance_indicators/
    ├── backends.csv
    ├── frontends.csv
    ├── compute_nodes.csv
    ├── cluster_architecture.txt
    ├── oversized_tablets.csv
    ├── too_many_tablets.csv
    ├── data_skew.csv
    ├── partition_data_skew.csv
    ├── empty_tables.csv
    ├── empty_partitions.csv
    ├── replication_check.csv
    └── summary.txt
```

### Key Files
- summary.txt - High-level overview of detected issues.
- <table>.sql - Table schemas.
- <table>_partitions.csv - Partition information.
- <table>_tablets.csv - Tablet metadata.



## Known Limitations
- Does not currently detect tables with incomplete or failed replicas.
- Assumes all databases in a catalog should be included (no exclusion filter).
- Assumes partition and tablet data is available via `information_schema` (requires appropriate StarRocks version).



## Future Improvements
- Add more comprehensive health checks (e.g., data corruption, index issues).
- Integrate with Prometheus metrics for real-time insights.
- Support for multi-cluster environments.
- Option to generate HTML or PDF reports for non-technical stakeholders.



## Contributing
Contributions are welcome! Please submit pull requests or open issues on GitHub.



## License
Apache License, Version 2.0 