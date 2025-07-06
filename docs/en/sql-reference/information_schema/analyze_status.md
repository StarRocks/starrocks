---
displayed_sidebar: docs
---

# analyze_status

`analyze_status` provides information about the status of analyze jobs.

The following fields are provided in `analyze_status`:

| **Field**    | **Description**                                              |
| ------------ | ------------------------------------------------------------ |
| Id           | ID of the analyze job.                                       |
| Catalog      | Catalog where the table belongs.                             |
| Database     | Database where the table belongs.                            |
| Table        | Name of the table being analyzed.                            |
| Columns      | Columns being analyzed.                                      |
| Type         | Type of analyze job. Valid values: `FULL`, `SAMPLE`.         |
| Schedule     | Schedule type of the analyze job. Valid values: `ONCE`, `AUTOMATIC`. |
| Status       | Status of the analyze job. Valid values: `PENDING`, `RUNNING`, `FINISH`, `FAILED`. |
| StartTime    | Start time of the analyze job.                               |
| EndTime      | End time of the analyze job.                                 |
| Properties   | Properties of the analyze job.                               |
| Reason       | Reason for the analyze job status.                           |
