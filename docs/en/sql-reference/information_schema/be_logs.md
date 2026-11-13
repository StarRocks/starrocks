---
displayed_sidebar: docs
<<<<<<< HEAD
=======
description: "be_logs provides information about the logs of each BE node."
products: [oss, enterprise, cloud, anywhere]
>>>>>>> 7fbe5e8 ([Doc] Add the products frontmatter line to the docs pages (#83243))
---

# be_logs

`be_logs` provides information about the logs of each BE node.

The following fields are provided in `be_logs`:

| **Field**   | **Description**                                              |
| ----------- | ------------------------------------------------------------ |
| BE_ID       | ID of the BE node.                                           |
| LEVEL       | Log level (e.g., `INFO`, `WARNING`, `ERROR`).                |
| TIMESTAMP   | Timestamp of the log entry.                                  |
| TID         | Thread ID that generated the log entry.                      |
| LOG         | Log message content.                                         |
