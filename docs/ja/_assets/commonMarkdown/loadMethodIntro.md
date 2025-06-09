- 同期ロードには [INSERT](../../sql-reference/sql-statements/loading_unloading/INSERT.md) + [`FILES()`](../../sql-reference/sql-functions/table-functions/files.md) を使用
- 非同期ロードには [Broker Load](../../sql-reference/sql-statements/loading_unloading/BROKER_LOAD.md) を使用
- 継続的な非同期ロードには [Pipe](../../sql-reference/sql-statements/loading_unloading/pipe/CREATE_PIPE.md) を使用

これらの各オプションにはそれぞれの利点があり、以下のセクションで詳しく説明されています。

ほとんどの場合、使用が非常に簡単な INSERT+`FILES()` メソッドを使用することをお勧めします。

ただし、INSERT+`FILES()` メソッドは現在、Parquet、ORC、および CSV ファイル形式のみをサポートしています。したがって、JSON などの他のファイル形式のデータをロードする必要がある場合や、データロード中に DELETE などのデータ変更を行う必要がある場合は、Broker Load を利用できます。

大量のデータファイルを合計で大規模なデータ量（例えば、100 GB 以上または 1 TB 以上）でロードする必要がある場合は、Pipe メソッドを使用することをお勧めします。Pipe はファイルの数やサイズに基づいてファイルを分割し、ロードジョブをより小さな連続タスクに分解します。このアプローチにより、1 つのファイルでエラーが発生しても全体のロードジョブに影響を与えず、データエラーによる再試行の必要性を最小限に抑えることができます。