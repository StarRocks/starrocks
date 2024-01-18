
- 使用 [INSERT](../../sql-reference/sql-statements/data-manipulation/INSERT.md)+[`FILES()`](../../sql-reference/sql-functions/table-functions/files.md) 进行同步导入。
- 使用 [Broker Load](../../sql-reference/sql-statements/data-manipulation/BROKER_LOAD.md) 进行异步导入。
- 使用 [Pipe](../../sql-reference/sql-statements/data-manipulation/CREATE_PIPE.md) 进行持续的异步导入。

三种导入方式各有优势，具体将在下面分章节详细阐述。

一般情况下，建议您使用 INSERT+`FILES()`，更为方便易用。

但是，INSERT+`FILES()` 当前只支持 Parquet 和 ORC 文件格式。因此，如果您需要导入其他格式（如 CSV）的数据、或者需要[在导入过程中执行 DELETE 等数据变更操作](../../loading/Load_to_Primary_Key_tables.md)，可以使用 Broker Load。

如果需要导入超大数据（比如超过 100 GB、特别是 1 TB 以上的数据量），建议您使用 Pipe。Pipe 会按文件数量或大小，自动对目录下的文件进行拆分，将一个大的导入作业拆分成多个较小的串行的导入任务，从而降低出错重试的代价。另外，在进行持续性的数据导入时，也推荐使用 Pipe，它能监听远端存储目录的文件变化，并持续导入有变化的文件数据。
