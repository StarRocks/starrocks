---
displayed_sidebar: docs
---

# Stream Load

## 1. Stream Load は、CSV 形式ファイルの最初の数行に含まれる列名の識別や、データ読み取り時に最初の数行をスキップすることをサポートしていますか？

Stream Load は、CSV 形式ファイルの最初の数行に含まれる列名の識別をサポートしていません。Stream Load は、最初の数行を他の行と同様に通常のデータとして扱います。

バージョン 2.5 以前では、Stream Load はデータ読み取り時に CSV ファイルの最初の数行をスキップすることをサポートしていません。ロードしたい CSV ファイルの最初の数行に列名が含まれている場合、以下のいずれかの方法を取ってください。

- データをエクスポートするために使用するツールの設定を変更します。そして、最初の数行に列名が含まれない CSV ファイルとしてデータを再エクスポートします。
- `sed -i '1d' filename` などのコマンドを使用して、CSV ファイルの最初の数行を削除します。
- ロードコマンドまたはステートメントで、`-H "where: <column_name> != '<column_name>'"` を使用して、CSV ファイルの最初の数行をフィルタリングします。`<column_name>` は最初の数行に含まれる任意の列名です。StarRocks は最初にソースデータを変換し、その後フィルタリングを行います。そのため、最初の数行の列名が対応するデータ型に変換されない場合、それらには `NULL` 値が返されます。これは、StarRocks の宛先テーブルに `NOT NULL` に設定された列を含めることができないことを意味します。
- ロードコマンドまたはステートメントで、`-H "max_filter_ratio:0.01"` を追加して、1% 以下の最大エラー許容値を設定し、最初の数行のデータ変換の失敗を無視できるようにします。この場合、`ErrorURL` がエラー行を示すために返されても、Stream Load ジョブは成功することができます。`max_filter_ratio` を大きな値に設定しないでください。大きな値に設定すると、重要なデータ品質の問題が見逃される可能性があります。

バージョン 3.0 以降、Stream Load は `skip_header` パラメータをサポートしており、CSV ファイルの最初の数行をスキップするかどうかを指定できます。詳細については、[CSV parameters](../../sql-reference/sql-statements/loading_unloading/STREAM_LOAD.md#csv-parameters) を参照してください。

## 2. パーティション列にロードするデータが標準の DATE または INT 型ではありません。例えば、データが 202106.00 のような形式です。Stream Load を使用してデータをロードする場合、どのようにデータを変換すればよいですか？

StarRocks はロード時にデータの変換をサポートしています。詳細については、[Transform data at loading](../../loading/Etl_in_loading.md) を参照してください。

例えば、`TEST` という名前の CSV 形式ファイルをロードしたいとします。このファイルは `NO`、`DATE`、`VERSION`、`PRICE` の4つの列で構成されており、その中の `DATE` 列のデータが 202106.00 のような非標準形式です。StarRocks で `DATE` をパーティション列として使用したい場合、まず StarRocks テーブルを作成する必要があります。例えば、次の4つの列を持つテーブルを作成します: `NO`、`VERSION`、`PRICE`、`DATE`。次に、StarRocks テーブルの `DATE` 列のデータ型を DATE、DATETIME、または INT として指定します。最後に、Stream Load ジョブを作成する際に、ロードコマンドまたはステートメントで次の設定を指定して、ソース `DATE` 列のデータ型を宛先列のデータ型に変換します。

```Plain
-H "columns: NO,DATE_1, VERSION, PRICE, DATE=LEFT(DATE_1,6)"
```

上記の例では、`DATE_1` は宛先 `DATE` 列にマッピングされる一時的な名前の列と見なすことができ、宛先 `DATE` 列にロードされる最終結果は `left()` 関数によって計算されます。まずソース列の一時的な名前をリストし、その後に関数を使用してデータを変換する必要があります。サポートされている関数はスカラー関数であり、非集計関数やウィンドウ関数を含みます。

## 3. Stream Load ジョブが "body exceed max size: 10737418240, limit: 10737418240" エラーを報告した場合、どうすればよいですか？

ソースデータファイルのサイズが 10 GB を超えており、これは Stream Load がサポートする最大ファイルサイズです。以下のいずれかの方法を取ってください。

- `seq -w 0 n` を使用して、ソースデータファイルをより小さなファイルに分割します。
- `curl -XPOST http://be_host:http_port/api/update_config?streaming_load_max_mb=<file_size>` を使用して、[BE configuration item](../../administration/management/BE_configuration.md#configure-be-dynamic-parameters) `streaming_load_max_mb` の値を調整し、最大ファイルサイズを増やします。