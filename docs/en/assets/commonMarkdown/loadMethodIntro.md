
- Synchronous loading using [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md)+[`FILES()`](../../sql-reference/sql-functions/table-functions/files.md)
- Asynchronous loading using [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)
- Continuous asynchronous loading using [Pipe](../../sql-reference/sql-statements/data-manipulation/CREATE_PIPE.md)

Each of these options has its own advantages, which are detailed in the following sections.

In most cases, we recommend that you use the INSERT+`FILES()` method, which is much easier to use.

However, the INSERT+`FILES()` method currently supports only the Parquet and ORC file formats. Therefore, if you need to load data of other file formats such as CSV, or [perform data changes such as DELETE during data loading](../../loading/Load_to_Primary_Key_tables.md), you can resort to Broker Load.

If you need to load a large number of data files with a significant data volume in total (for example, more than 100 GB or even 1 TB), we recommend that you use the Pipe method. Pipe can split the files based on their number or size, breaking down the load job into smaller, sequential tasks. This approach ensures that errors in one file do not impact the entire load job and minimizes the need for retries due to data errors.
