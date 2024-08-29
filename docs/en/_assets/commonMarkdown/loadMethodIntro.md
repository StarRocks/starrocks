
<<<<<<< HEAD
- Synchronous loading using [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md)+[`FILES()`](../../sql-reference/sql-functions/table-functions/files.md)
- Asynchronous loading using [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md)
=======
- Synchronous loading using [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md)+[`FILES()`](../../sql-reference/sql-functions/table-functions/files.md)
- Asynchronous loading using [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md)
- Continuous asynchronous loading using [Pipe](../../sql-reference/sql-statements/loading_unloading/pipe/CREATE_PIPE.md)
>>>>>>> e06217c368 ([Doc] Ref docs (#50111))

Each of these options has its own advantages, which are detailed in the following sections.

In most cases, we recommend that you use the INSERT+`FILES()` method, which is much easier to use.

However, the INSERT+`FILES()` method currently supports only the Parquet and ORC file formats. Therefore, if you need to load data of other file formats such as CSV, or [perform data changes such as DELETE during data loading](../../loading/Load_to_Primary_Key_tables.md), you can resort to Broker Load.
