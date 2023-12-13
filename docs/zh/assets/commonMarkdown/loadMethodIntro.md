
- 使用 [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md)+[`FILES()`](../../sql-reference/sql-functions/table-functions/files.md) 进行同步导入。
- 使用 [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 进行异步导入。

两种导入方式各有优势，具体将在下面分章节详细阐述。

一般情况下，建议您使用 INSERT+`FILES()`，更为方便易用。

但是，INSERT+`FILES()` 当前只支持 Parquet 和 ORC 文件格式。因此，如果您需要导入其他格式（如 CSV）的数据、或者需要[在导入过程中执行 DELETE 等数据变更操作](../../loading/Load_to_Primary_Key_tables.md)，可以使用 Broker Load。
