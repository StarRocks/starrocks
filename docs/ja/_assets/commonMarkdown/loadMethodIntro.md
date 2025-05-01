- 同期ロードには [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) + [`FILES()`](../../sql-reference/sql-functions/table-functions/files.md) を使用
- 非同期ロードには [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を使用

これらのオプションにはそれぞれの利点があり、以下のセクションで詳しく説明します。

ほとんどの場合、使用が非常に簡単な INSERT+`FILES()` メソッドをお勧めします。

ただし、INSERT+`FILES()` メソッドは現在、Parquet と ORC ファイル形式のみをサポートしています。したがって、CSV などの他のファイル形式のデータをロードする必要がある場合や、データロード中に DELETE などのデータ変更を行う必要がある場合は、Broker Load を利用できます。