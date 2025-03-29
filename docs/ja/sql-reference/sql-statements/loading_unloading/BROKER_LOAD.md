---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# BROKER LOAD

import InsertPrivNote from '../../../_assets/commonMarkdown/insertPrivNote.md'

## 説明

StarRocks は、MySQL ベースのロード方法である Broker Load を提供します。ロードジョブを送信すると、StarRocks は非同期でジョブを実行します。`SELECT * FROM information_schema.loads` を使用してジョブの結果をクエリすることができます。この機能は v3.1 以降でサポートされています。背景情報、原則、サポートされているデータファイル形式、単一テーブルロードと複数テーブルロードの実行方法、ジョブ結果の表示方法については、[loading overview](../../../loading/Loading_intro.md) を参照してください。

<InsertPrivNote />

## 構文

```SQL
LOAD LABEL [<database_name>.]<label_name>
(
    data_desc[, data_desc ...]
)
WITH BROKER
(
    StorageCredentialParams
)
[PROPERTIES
(
    opt_properties
)
]
```

StarRocks では、いくつかのリテラルが SQL 言語によって予約キーワードとして使用されます。これらのキーワードを SQL ステートメントで直接使用しないでください。SQL ステートメントでそのようなキーワードを使用する場合は、バッククォート (`) で囲んでください。[Keywords](../keywords.md) を参照してください。

## パラメータ

### database_name と label_name

`label_name` はロードジョブのラベルを指定します。命名規則については、[System limits](../../System_limit.md) を参照してください。

`database_name` は、オプションで、宛先テーブルが属するデータベースの名前を指定します。

各ロードジョブには、データベース全体で一意のラベルがあります。ロードジョブのラベルを使用して、ロードジョブの実行ステータスを表示し、同じデータを繰り返しロードするのを防ぐことができます。ロードジョブが **FINISHED** 状態になると、そのラベルは再利用できません。**CANCELLED** 状態になったロードジョブのラベルのみが再利用できます。ほとんどの場合、ロードジョブのラベルは、そのロードジョブを再試行して同じデータをロードするために再利用され、Exactly-Once セマンティクスを実装します。

ラベルの命名規則については、[System limits](../../System_limit.md) を参照してください。

### data_desc

ロードするデータのバッチの説明です。各 `data_desc` ディスクリプタは、データソース、ETL 関数、宛先 StarRocks テーブル、および宛先パーティションなどの情報を宣言します。

Broker Load は、一度に複数のデータファイルをロードすることをサポートしています。1 つのロードジョブで、複数の `data_desc` ディスクリプタを使用してロードしたい複数のデータファイルを宣言するか、1 つの `data_desc` ディスクリプタを使用して、すべてのデータファイルをロードしたいファイルパスを宣言することができます。Broker Load は、複数のデータファイルをロードする各ロードジョブのトランザクションの原子性も保証します。原子性とは、1 つのロードジョブで複数のデータファイルのロードがすべて成功するか失敗するかのいずれかであることを意味します。いくつかのデータファイルのロードが成功し、他のファイルのロードが失敗することはありません。

`data_desc` は次の構文をサポートしています：

```SQL
DATA INFILE ("<file_path>"[, "<file_path>" ...])
[NEGATIVE]
INTO TABLE <table_name>
[PARTITION (<partition1_name>[, <partition2_name> ...])]
[TEMPORARY PARTITION (<temporary_partition1_name>[, <temporary_partition2_name> ...])]
[COLUMNS TERMINATED BY "<column_separator>"]
[ROWS TERMINATED BY "<row_separator>"]
[FORMAT AS "CSV | Parquet | ORC"]
[(format_type_options)]
[(column_list)]
[COLUMNS FROM PATH AS (<partition_field_name>[, <partition_field_name> ...])]
[SET <k1=f1(v1)>[, <k2=f2(v2)> ...]]
[WHERE predicate]
```

`data_desc` には次のパラメータが含まれている必要があります：

- `file_path`

  ロードしたい 1 つ以上のデータファイルの保存パスを指定します。

  このパラメータを 1 つのデータファイルの保存パスとして指定できます。たとえば、HDFS サーバー上の `/user/data/tablename` パスから `20210411` という名前のデータファイルをロードするために、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"` として指定できます。

  また、ワイルドカード `?`、`*`、`[]`、`{}`、または `^` を使用して、複数のデータファイルの保存パスとしてこのパラメータを指定することもできます。[Wildcard reference](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#globStatus-org.apache.hadoop.fs.Path-) を参照してください。たとえば、HDFS サーバー上の `/user/data/tablename` パス内のすべてのパーティションまたは `202104` パーティションのみからデータファイルをロードするために、このパラメータを `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"` または `"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"` として指定できます。

  > **注意**
  >
  > ワイルドカードは、中間パスを指定するためにも使用できます。

  上記の例では、`hdfs_host` と `hdfs_port` パラメータは次のように説明されています：

  - `hdfs_host`: HDFS クラスター内の NameNode ホストの IP アドレス。

  - `hdfs_port`: HDFS クラスター内の NameNode ホストの FS ポート。デフォルトのポート番号は `9000` です。

  > **注意**
  >
  > - Broker Load は、S3 または S3A プロトコルに従って AWS S3 へのアクセスをサポートしています。したがって、AWS S3 からデータをロードする場合、S3 URI に `s3://` または `s3a://` をプレフィックスとして含めることができます。
  > - Broker Load は、gs プロトコルに従ってのみ Google GCS へのアクセスをサポートしています。したがって、Google GCS からデータをロードする場合、GCS URI に `gs://` をプレフィックスとして含める必要があります。
  > - Blob Storage からデータをロードする場合、wasb または wasbs プロトコルを使用してデータにアクセスする必要があります：
  >   - ストレージアカウントが HTTP 経由でのアクセスを許可している場合、wasb プロトコルを使用し、ファイルパスを `wasb://<container_name>@<storage_account_name>.blob.core.windows.net/<path>/<file_name>/*` として記述します。
  >   - ストレージアカウントが HTTPS 経由でのアクセスを許可している場合、wasbs プロトコルを使用し、ファイルパスを `wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/<path>/<file_name>/*` として記述します。
  > - Data Lake Storage Gen2 からデータをロードする場合、abfs または abfss プロトコルを使用してデータにアクセスする必要があります：
  >   - ストレージアカウントが HTTP 経由でのアクセスを許可している場合、abfs プロトコルを使用し、ファイルパスを `abfs://<container_name>@<storage_account_name>.dfs.core.windows.net/<file_name>` として記述します。
  >   - ストレージアカウントが HTTPS 経由でのアクセスを許可している場合、abfss プロトコルを使用し、ファイルパスを `abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<file_name>` として記述します。
  > - Data Lake Storage Gen1 からデータをロードする場合、adl プロトコルを使用してデータにアクセスし、ファイルパスを `adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>` として記述します。

- `INTO TABLE`

  宛先 StarRocks テーブルの名前を指定します。

`data_desc` は、次のパラメータをオプションで含めることができます：

- `NEGATIVE`

  特定のデータバッチのロードを取り消します。これを達成するには、`NEGATIVE` キーワードを指定して同じデータバッチをロードする必要があります。

  > **注意**
  >
  > このパラメータは、StarRocks テーブルが集計テーブルであり、そのすべての値列が `sum` 関数によって計算される場合にのみ有効です。

- `PARTITION`

   データをロードしたいパーティションを指定します。デフォルトでは、このパラメータを指定しない場合、ソースデータは StarRocks テーブルのすべてのパーティションにロードされます。

- `TEMPORARY PARTITION`

  データをロードしたい [temporary partition](../../../table_design/data_distribution/Temporary_partition.md) の名前を指定します。複数の一時パーティションを指定することができ、それらはカンマ (,) で区切る必要があります。

- `COLUMNS TERMINATED BY`

  データファイルで使用される列区切り文字を指定します。デフォルトでは、このパラメータを指定しない場合、このパラメータはタブを示す `\t` にデフォルト設定されます。このパラメータを使用して指定する列区切り文字は、データファイルで実際に使用されている列区切り文字と同じでなければなりません。そうでない場合、データ品質が不十分なためロードジョブは失敗し、その `State` は `CANCELLED` になります。

  Broker Load ジョブは MySQL プロトコルに従って送信されます。StarRocks と MySQL の両方がロードリクエストで文字をエスケープします。したがって、列区切り文字がタブのような不可視文字である場合、列区切り文字の前にバックスラッシュ (\) を追加する必要があります。たとえば、列区切り文字が `\t` の場合は `\\t` を入力する必要があり、列区切り文字が `\n` の場合は `\\n` を入力する必要があります。Apache Hive™ ファイルは `\x01` を列区切り文字として使用するため、データファイルが Hive からのものである場合は `\\x01` を入力する必要があります。

  > **注意**
  >
  > - CSV データの場合、UTF-8 文字列（カンマ (,) やタブ、パイプ (|) など）をテキスト区切り文字として使用できますが、その長さは 50 バイトを超えてはなりません。
  > - Null 値は `\N` を使用して示されます。たとえば、データファイルが 3 列で構成され、そのデータファイルのレコードが最初と最後の列にデータを持ち、2 番目の列にデータがない場合、この状況では 2 番目の列に `\N` を使用して null 値を示す必要があります。これは、レコードを `a,\N,b` としてコンパイルする必要があることを意味し、`a,,b` ではありません。`a,,b` は、レコードの 2 番目の列が空の文字列を持っていることを示します。

- `ROWS TERMINATED BY`

  データファイルで使用される行区切り文字を指定します。デフォルトでは、このパラメータを指定しない場合、このパラメータは改行を示す `\n` にデフォルト設定されます。このパラメータを使用して指定する行区切り文字は、データファイルで実際に使用されている行区切り文字と同じでなければなりません。そうでない場合、データ品質が不十分なためロードジョブは失敗し、その `State` は `CANCELLED` になります。このパラメータは v2.5.4 以降でサポートされています。

  行区切り文字の使用に関する注意事項については、前述の `COLUMNS TERMINATED BY` パラメータの使用に関する注意事項を参照してください。

- `FORMAT AS`

  データファイルの形式を指定します。有効な値は `CSV`、`Parquet`、および `ORC` です。デフォルトでは、このパラメータを指定しない場合、StarRocks は `file_path` パラメータで指定されたファイル名拡張子 **.csv**、**.parquet**、または **.orc** に基づいてデータファイル形式を決定します。

- `format_type_options`

  `FORMAT AS` が `CSV` に設定されている場合の CSV 形式オプションを指定します。構文：

  ```JSON
  (
      key = value
      key = value
      ...
  )
  ```

  > **注意**
  >
  > `format_type_options` は v3.0 以降でサポートされています。

  次の表は、オプションを説明しています。

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| skip_header   | CSV 形式のデータファイルの最初の行をスキップするかどうかを指定します。タイプ: INTEGER。デフォルト値: `0`。<br />一部の CSV 形式のデータファイルでは、最初の行は列名や列データ型などのメタデータを定義するために使用されます。`skip_header` パラメータを設定することで、StarRocks がデータロード中にデータファイルの最初の行をスキップするようにできます。たとえば、このパラメータを `1` に設定すると、StarRocks はデータロード中にデータファイルの最初の行をスキップします。<br />データファイルの最初の行は、ロードステートメントで指定した行区切り文字を使用して区切られている必要があります。 |
| trim_space    | CSV 形式のデータファイルから列区切り文字の前後のスペースを削除するかどうかを指定します。タイプ: BOOLEAN。デフォルト値: `false`。<br />一部のデータベースでは、データを CSV 形式のデータファイルとしてエクスポートする際に列区切り文字にスペースが追加されます。これらのスペースは、先行スペースまたは後続スペースと呼ばれます。`trim_space` パラメータを設定することで、StarRocks がデータロード中にこれらの不要なスペースを削除するようにできます。<br />ただし、StarRocks は、`enclose` で指定された文字で囲まれたフィールド内のスペース（先行スペースおよび後続スペースを含む）を削除しません。たとえば、次のフィールド値は、パイプ (`|`) を列区切り文字として使用し、二重引用符 (`"`) を `enclose` で指定された文字として使用しています：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124; "Love StarRocks" &#124;</code> <br />`trim_space` を `true` に設定すると、StarRocks は次のようにフィールド値を処理します：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> |
| enclose       | CSV 形式のデータファイルでフィールド値を [RFC4180](https://www.rfc-editor.org/rfc/rfc4180) に従って囲むために使用される文字を指定します。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字は単一引用符 (`'`) および二重引用符 (`"`) です。<br />`enclose` で指定された文字で囲まれたすべての特殊文字（行区切り文字や列区切り文字を含む）は通常の記号と見なされます。StarRocks は、`enclose` で指定された文字として任意の単一バイト文字を指定できるため、RFC4180 よりも多くのことができます。<br />フィールド値に `enclose` で指定された文字が含まれている場合、同じ文字を使用してその `enclose` で指定された文字をエスケープできます。たとえば、`enclose` を `"` に設定し、フィールド値が `a "quoted" c` の場合、このフィールド値をデータファイルに `"a ""quoted"" c"` として入力できます。 |
| escape        | 行区切り文字、列区切り文字、エスケープ文字、`enclose` で指定された文字などのさまざまな特殊文字をエスケープするために使用される文字を指定します。これらの文字は、StarRocks によって通常の文字と見なされ、それらが存在するフィールド値の一部として解析されます。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ (`\`) で、SQL ステートメントでは二重スラッシュ (`\\`) として記述する必要があります。<br />**注意**<br />`escape` で指定された文字は、`enclose` で指定された文字のペアの内側と外側の両方に適用されます。<br />次の 2 つの例があります：<ul><li>`enclose` を `"` に設定し、`escape` を `\` に設定すると、StarRocks は `"say \"Hello world\""` を `say "Hello world"` に解析します。</li><li>列区切り文字がカンマ (`,`) の場合、`escape` を `\` に設定すると、StarRocks は `a, b\, c` を 2 つの別々のフィールド値 `a` と `b, c` に解析します。</li></ul> |

- `column_list`

  データファイルと StarRocks テーブルの間の列マッピングを指定します。構文: `(<column_name>[, <column_name> ...])`。`column_list` で宣言された列は、名前によって StarRocks テーブルの列にマッピングされます。

  > **注意**
  >
  > データファイルの列が StarRocks テーブルの列に順番にマッピングされる場合、`column_list` を指定する必要はありません。

  データファイルの特定の列をスキップしたい場合、その列を一時的に StarRocks テーブルの列とは異なる名前にするだけで済みます。詳細については、[loading overview](../../../loading/Loading_intro.md) を参照してください。

- `COLUMNS FROM PATH AS`

  指定したファイルパスから 1 つ以上のパーティションフィールドに関する情報を抽出します。このパラメータは、ファイルパスにパーティションフィールドが含まれている場合にのみ有効です。

  たとえば、データファイルが `/path/col_name=col_value/file1` に保存されている場合、`col_name` はパーティションフィールドであり、StarRocks テーブルの列にマッピングできます。このパラメータを `col_name` として指定することができます。このようにして、StarRocks はパスから `col_value` 値を抽出し、それらを `col_name` にマッピングされた StarRocks テーブル列にロードします。

  > **注意**
  >
  > このパラメータは、HDFS からデータをロードする場合にのみ使用できます。

- `SET`

  データファイルの列を変換するために使用したい 1 つ以上の関数を指定します。例：

  - StarRocks テーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。データファイルは 4 つの列で構成されており、そのうち最初の 2 つの列は StarRocks テーブルの `col1` と `col2` に順番にマッピングされ、最後の 2 つの列の合計が StarRocks テーブルの `col3` にマッピングされます。この場合、`column_list` を `(col1,col2,tmp_col3,tmp_col4)` として指定し、SET 句で `(col3=tmp_col3+tmp_col4)` を指定してデータ変換を実装する必要があります。
  - StarRocks テーブルは、順番に `year`、`month`、`day` の 3 つの列で構成されています。データファイルは、`yyyy-mm-dd hh:mm:ss` 形式の日付と時刻の値を含む 1 つの列のみで構成されています。この場合、`column_list` を `(tmp_time)` として指定し、SET 句で `(year = year(tmp_time), month=month(tmp_time), day=day(tmp_time))` を指定してデータ変換を実装する必要があります。

- `WHERE`

  ソースデータをフィルタリングするための条件を指定します。StarRocks は、WHERE 句で指定されたフィルタ条件を満たすソースデータのみをロードします。

### WITH BROKER

v2.3 以前では、`WITH BROKER "<broker_name>"` を入力して使用したいブローカーを指定します。v2.5 以降では、ブローカーを指定する必要はありませんが、`WITH BROKER` キーワードを保持する必要があります。

### StorageCredentialParams

StarRocks がストレージシステムにアクセスするために使用する認証情報。

#### HDFS

オープンソースの HDFS は、シンプル認証と Kerberos 認証の 2 つの認証方法をサポートしています。Broker Load はデフォルトでシンプル認証を使用します。オープンソースの HDFS は、NameNode の HA メカニズムの構成もサポートしています。ストレージシステムとしてオープンソースの HDFS を選択する場合、認証構成と HA 構成を次のように指定できます：

- 認証構成

  - シンプル認証を使用する場合、`StorageCredentialParams` を次のように構成します：

    ```Plain
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
    ```

    `StorageCredentialParams` のパラメータは次のように説明されています。

    | Parameter                       | Description                                                  |
    | ------------------------------- | ------------------------------------------------------------ |
    | hadoop.security.authentication  | 認証方法。有効な値: `simple` および `kerberos`。デフォルト値: `simple`。`simple` はシンプル認証を表し、認証なしを意味し、`kerberos` は Kerberos 認証を表します。 |
    | username                        | HDFS クラスターの NameNode にアクセスするために使用するアカウントのユーザー名。 |
    | password                        | HDFS クラスターの NameNode にアクセスするために使用するアカウントのパスワード。 |

  - Kerberos 認証を使用する場合、`StorageCredentialParams` を次のように構成します：

    ```Plain
    "hadoop.security.authentication" = "kerberos",
    "kerberos_principal" = "nn/zelda1@ZELDA.COM",
    "kerberos_keytab" = "/keytab/hive.keytab",
    "kerberos_keytab_content" = "YWFhYWFh"
    ```

    `StorageCredentialParams` のパラメータは次のように説明されています。

    | Parameter                       | Description                                                  |
    | ------------------------------- | ------------------------------------------------------------ |
    | hadoop.security.authentication  | 認証方法。有効な値: `simple` および `kerberos`。デフォルト値: `simple`。`simple` はシンプル認証を表し、認証なしを意味し、`kerberos` は Kerberos 認証を表します。 |
    | kerberos_principal              | 認証される Kerberos プリンシパル。各プリンシパルは、HDFS クラスター全体で一意であることを保証するために、次の 3 つの部分で構成されています：<ul><li>`username` または `servicename`: プリンシパルの名前。</li><li>`instance`: HDFS クラスター内の認証されるノードをホストするサーバーの名前。サーバー名は、たとえば、HDFS クラスターが独立して認証される複数の DataNode で構成されている場合に、プリンシパルが一意であることを保証するのに役立ちます。</li><li>`realm`: レルムの名前。レルム名は大文字でなければなりません。</li></ul>例: `nn/zelda1@ZELDA.COM`。 |
    | kerberos_keytab                 | Kerberos キータブファイルの保存パス。 |
    | kerberos_keytab_content         | Kerberos キータブファイルの Base64 エンコードされた内容。`kerberos_keytab` または `kerberos_keytab_content` のいずれかを指定することができます。 |

- HA 構成

  HDFS クラスターの NameNode に HA メカニズムを構成できます。これにより、NameNode が別のノードに切り替えられた場合でも、StarRocks は自動的に新しい NameNode を識別できます。これには次のシナリオが含まれます：

  - 1 つの Kerberos ユーザーが構成された単一の HDFS クラスターからデータをロードする場合、ブローカー ベースのロードとブローカー フリーのロードの両方がサポートされています。
  
    - ブローカー ベースのロードを実行するには、少なくとも 1 つの独立したブローカー グループがデプロイされていることを確認し、`hdfs-site.xml` ファイルを HDFS クラスターを提供するブローカー ノードの `{deploy}/conf` パスに配置します。StarRocks は、ブローカーの起動時に `{deploy}/conf` パスを環境変数 `CLASSPATH` に追加し、ブローカーが HDFS クラスター ノードに関する情報を読み取れるようにします。
  
    - ブローカー フリーのロードを実行するには、クラスター内のすべての FE、BE、および CN ノードのデプロイメント ディレクトリの `conf/core-site.xml` に `hadoop.security.authentication = kerberos` を設定し、`kinit` コマンドを使用して Kerberos アカウントを構成するだけで済みます。
  
  - 複数の Kerberos ユーザーが構成された単一の HDFS クラスターからデータをロードする場合、ブローカー ベースのロードのみがサポートされています。少なくとも 1 つの独立したブローカー グループがデプロイされていることを確認し、`hdfs-site.xml` ファイルを HDFS クラスターを提供するブローカー ノードの `{deploy}/conf` パスに配置します。StarRocks は、ブローカーの起動時に `{deploy}/conf` パスを環境変数 `CLASSPATH` に追加し、ブローカーが HDFS クラスター ノードに関する情報を読み取れるようにします。

  - 複数の HDFS クラスターからデータをロードする場合（1 つまたは複数の Kerberos ユーザーが構成されているかどうかにかかわらず）、ブローカー ベースのロードのみがサポートされています。これらの HDFS クラスターのそれぞれに対して少なくとも 1 つの独立したブローカー グループがデプロイされていることを確認し、ブローカーが HDFS クラスター ノードに関する情報を読み取れるようにするために次のいずれかのアクションを実行します：

    - `hdfs-site.xml` ファイルを HDFS クラスターを提供するブローカー ノードの `{deploy}/conf` パスに配置します。StarRocks は、ブローカーの起動時に `{deploy}/conf` パスを環境変数 `CLASSPATH` に追加し、その HDFS クラスター内のノードに関する情報を読み取れるようにします。

    - ジョブ作成時に次の HA 構成を追加します：

      ```Plain
      "dfs.nameservices" = "ha_cluster",
      "dfs.ha.namenodes.ha_cluster" = "ha_n1,ha_n2",
      "dfs.namenode.rpc-address.ha_cluster.ha_n1" = "<hdfs_host>:<hdfs_port>",
      "dfs.namenode.rpc-address.ha_cluster.ha_n2" = "<hdfs_host>:<hdfs_port>",
      "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      ```

      HA 構成のパラメータは次のように説明されています。

| Parameter                          | Description                                                  |
| ---------------------------------- | ------------------------------------------------------------ |
| dfs.nameservices                   | HDFS クラスターの名前。                                       |
| dfs.ha.namenodes.XXX               | HDFS クラスター内の NameNode の名前。複数の NameNode 名を指定する場合は、カンマ (`,`) で区切ります。`xxx` は `dfs.nameservices` で指定した HDFS クラスター名です。 |
| dfs.namenode.rpc-address.XXX.NN    | HDFS クラスター内の NameNode の RPC アドレス。`NN` は `dfs.ha.namenodes.XXX` で指定した NameNode 名です。 |
| dfs.client.failover.proxy.provider | クライアントが接続する NameNode のプロバイダー。デフォルト値: `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`。 |

  > **注意**
  >
  > StarRocks クラスターにデプロイされているブローカーを確認するには、[SHOW BROKER](../cluster-management/nodes_processes/SHOW_BROKER.md) ステートメントを使用できます。

#### AWS S3

ストレージシステムとして AWS S3 を選択する場合、次のいずれかのアクションを実行します：

- インスタンスプロファイルベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインドロールベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAM ユーザーベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

`StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

| Parameter                   | Required | Description                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | Yes      | 資格情報メソッドのインスタンスプロファイルとアサインドロールを有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。 |
| aws.s3.iam_role_arn         | No       | AWS S3 バケットに対する権限を持つ IAM ロールの ARN。AWS S3 にアクセスするための資格情報メソッドとしてアサインドロールを選択する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | Yes      | AWS S3 バケットが存在するリージョン。例: `us-west-1`。        |
| aws.s3.access_key           | No       | IAM ユーザーのアクセスキー。AWS S3 にアクセスするための資格情報メソッドとして IAM ユーザーを選択する場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | No       | IAM ユーザーのシークレットキー。AWS S3 にアクセスするための資格情報メソッドとして IAM ユーザーを選択する場合、このパラメータを指定する必要があります。 |

AWS S3 へのアクセスのための認証方法の選択方法と AWS IAM コンソールでのアクセス制御ポリシーの設定方法については、[Authentication parameters for accessing AWS S3](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3) を参照してください。

#### Google GCS

ストレージシステムとして Google GCS を選択する場合、次のいずれかのアクションを実行します：

- VM ベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされているサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | サービスアカウントの作成時に生成された JSON ファイルのメールアドレス。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成された JSON ファイルのプライベートキー。 |

- インパーソネーションベースの認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  - VM インスタンスにサービスアカウントをインパーソネートさせる：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

    | **Parameter**                              | **Default value** | **Value** **example** | **Description**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engine にバインドされているサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウント。                   |

  - サービスアカウント（メタサービスアカウントと呼ばれる）に別のサービスアカウント（データサービスアカウントと呼ばれる）をインパーソネートさせる：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

    | **Parameter**                          | **Default value** | **Value** **example**                                        | **Description**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | メタサービスアカウントの作成時に生成された JSON ファイルのメールアドレス。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー ID。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成された JSON ファイルのプライベートキー。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウント。             |

#### その他の S3 互換ストレージシステム

MinIO などの他の S3 互換ストレージシステムを選択する場合、`StorageCredentialParams` を次のように構成します：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

| Parameter                        | Required | Description                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | Yes      | SSL 接続を有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `true`。 |
| aws.s3.enable_path_style_access  | Yes      | パススタイルの URL アクセスを有効にするかどうかを指定します。有効な値: `true` および `false`。デフォルト値: `false`。MinIO の場合、値を `true` に設定する必要があります。 |
| aws.s3.endpoint                  | Yes      | AWS S3 ではなく、S3 互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key                | Yes      | IAM ユーザーのアクセスキー。                                 |
| aws.s3.secret_key                | Yes      | IAM ユーザーのシークレットキー。                             |

#### Microsoft Azure Storage

##### Azure Blob Storage

ストレージシステムとして Blob Storage を選択する場合、次のいずれかのアクションを実行します：

- 共有キー認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**              | **Required** | **Description**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | Yes          | Blob Storage アカウントのユーザー名。        |
  | azure.blob.shared_key      | Yes          | Blob Storage アカウントの共有キー。          |

- SAS トークン認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**             | **Required** | **Description**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| Yes          | Blob Storage アカウントのユーザー名。                        |
  | azure.blob.container      | Yes          | データを格納する Blob コンテナの名前。                       |
  | azure.blob.sas_token      | Yes          | Blob Storage アカウントにアクセスするために使用される SAS トークン。 |

##### Azure Data Lake Storage Gen2

ストレージシステムとして Data Lake Storage Gen2 を選択する場合、次のいずれかのアクションを実行します：

- マネージド ID 認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                           | **Required** | **Description**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | Yes          | マネージド ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |
  | azure.adls2.oauth2_tenant_id            | Yes          | アクセスしたいデータのテナント ID。                          |
  | azure.adls2.oauth2_client_id            | Yes          | マネージド ID のクライアント（アプリケーション）ID。         |

- 共有キー認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**               | **Required** | **Description**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | Yes          | Data Lake Storage Gen2 ストレージアカウントのユーザー名。    |
  | azure.adls2.shared_key      | Yes          | Data Lake Storage Gen2 ストレージアカウントの共有キー。      |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                      | **Required** | **Description**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | Yes          | サービスプリンシパルのクライアント（アプリケーション）ID。   |
  | azure.adls2.oauth2_client_secret   | Yes          | 作成された新しいクライアント（アプリケーション）シークレットの値。 |
  | azure.adls2.oauth2_client_endpoint | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント（v1）。 |

##### Azure Data Lake Storage Gen1

ストレージシステムとして Data Lake Storage Gen1 を選択する場合、次のいずれかのアクションを実行します：

- マネージドサービス ID 認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

  | **Parameter**                            | **Required** | **Description**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | Yes          | マネージドサービス ID 認証方法を有効にするかどうかを指定します。値を `true` に設定します。 |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams` を次のように構成します：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams` に設定する必要があるパラメータは次のように説明されています。

| **Parameter**                 | **Required** | **Description**                                              |
| ----------------------------- | ------------ | ------------------------------------------------------------ |
| azure.adls1.oauth2_client_id  | Yes          | クライアント（アプリケーション）ID。                         |
| azure.adls1.oauth2_credential | Yes          | 作成された新しいクライアント（アプリケーション）シークレットの値。 |
| azure.adls1.oauth2_endpoint   | Yes          | サービスプリンシパルまたはアプリケーションの OAuth 2.0 トークンエンドポイント（v1）。 |

### opt_properties

ロードジョブ全体に適用されるオプションのパラメータを指定します。構文：

```Plain
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

サポートされているパラメータは次のとおりです：

- `timeout`

  ロードジョブのタイムアウト期間を指定します。単位：秒。デフォルトのタイムアウト期間は 4 時間です。6 時間未満のタイムアウト期間を指定することをお勧めします。ロードジョブがタイムアウト期間内に完了しない場合、StarRocks はロードジョブをキャンセルし、ロードジョブのステータスは **CANCELLED** になります。

  > **注意**
  >
  > ほとんどの場合、タイムアウト期間を設定する必要はありません。ロードジョブがデフォルトのタイムアウト期間内に完了しない場合にのみ、タイムアウト期間を設定することをお勧めします。

  タイムアウト期間を推測するには、次の式を使用します：

  **タイムアウト期間 > (ロードするデータファイルの合計サイズ x ロードするデータファイルの合計数とデータファイルに作成されたマテリアライズドビュー)/平均ロード速度**

  > **注意**
  >
  > 「平均ロード速度」は、StarRocks クラスター全体の平均ロード速度です。平均ロード速度は、サーバー構成やクラスターに許可される最大同時クエリタスク数によって異なるため、クラスターごとに異なります。過去のロードジョブのロード速度に基づいて平均ロード速度を推測できます。

  たとえば、1 GB のデータファイルに 2 つのマテリアライズドビューが作成されている StarRocks クラスターにデータをロードしたい場合、クラスターの平均ロード速度が 10 MB/s であると仮定すると、データロードに必要な時間は約 102 秒です。

  (1 x 1024 x 3)/10 = 307.2 (秒)

  この例では、タイムアウト期間を 308 秒以上に設定することをお勧めします。

- `max_filter_ratio`

  ロードジョブの最大エラー許容度を指定します。最大エラー許容度は、データ品質が不十分なためにフィルタリングされる行の最大割合です。有効な値：`0`~`1`。デフォルト値：`0`。

  - このパラメータを `0` に設定すると、StarRocks はロード中に不適格な行を無視しません。そのため、ソースデータに不適格な行が含まれている場合、ロードジョブは失敗します。これにより、StarRocks にロードされるデータの正確性が保証されます。

  - このパラメータを `0` より大きい値に設定すると、StarRocks はロード中に不適格な行を無視できます。そのため、ソースデータに不適格な行が含まれていても、ロードジョブは成功することがあります。

    > **注意**
    >
    > データ品質が不十分なためにフィルタリングされる行には、WHERE 句によってフィルタリングされる行は含まれません。

  最大エラー許容度が `0` に設定されているためにロードジョブが失敗した場合、[SHOW LOAD](SHOW_LOAD.md) を使用してジョブ結果を表示できます。その後、不適格な行をフィルタリングできるかどうかを判断します。不適格な行をフィルタリングできる場合、ジョブ結果の `dpp.abnorm.ALL` と `dpp.norm.ALL` に返される値に基づいて最大エラー許容度を計算し、最大エラー許容度を調整してロードジョブを再送信します。最大エラー許容度を計算するための式は次のとおりです：

  `max_filter_ratio` = [`dpp.abnorm.ALL`/(`dpp.abnorm.ALL` + `dpp.norm.ALL`)]

  `dpp.abnorm.ALL` と `dpp.norm.ALL` に返される値の合計は、ロードされる行の総数です。

- `log_rejected_record_num`

  ログに記録できる不適格なデータ行の最大数を指定します。このパラメータは v3.1 以降でサポートされています。有効な値：`0`、`-1`、および任意の非ゼロの正の整数。デフォルト値：`0`。
  
  - 値 `0` は、フィルタリングされたデータ行がログに記録されないことを指定します。
  - 値 `-1` は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。
  - 非ゼロの正の整数 `n` は、フィルタリングされたデータ行が各 BE で最大 `n` 行までログに記録されることを指定します。

- `load_mem_limit`

  ロードジョブに提供できる最大メモリ量を指定します。このパラメータの値は、各 BE または CN ノードでサポートされる上限メモリを超えることはできません。単位：バイト。デフォルトのメモリ制限は 2 GB です。

- `strict_mode`

  [strict mode](../../../loading/load_concept/strict_mode.md) を有効にするかどうかを指定します。有効な値：`true` および `false`。デフォルト値：`false`。`true` は strict mode を有効にし、`false` は strict mode を無効にします。

- `timezone`

  ロードジョブのタイムゾーンを指定します。デフォルト値：`Asia/Shanghai`。タイムゾーンの設定は、strftime、alignment_timestamp、from_unixtime などの関数によって返される結果に影響を与えます。詳細については、[Configure a time zone](../../../administration/management/timezone.md) を参照してください。`timezone` パラメータで指定されたタイムゾーンは、セッションレベルのタイムゾーンです。

- `priority`

  ロードジョブの優先度を指定します。有効な値：`LOWEST`、`LOW`、`NORMAL`、`HIGH`、および `HIGHEST`。デフォルト値：`NORMAL`。Broker Load は FE パラメータ `max_broker_load_job_concurrency` を提供し、StarRocks クラスター内で同時に実行できる Broker Load ジョブの最大数を決定します。指定された時間内に送信された Broker Load ジョブの数が最大数を超える場合、過剰なジョブは優先度に基づいてスケジュールされるまで待機します。

  [ALTER LOAD](ALTER_LOAD.md) ステートメントを使用して、`QUEUEING` または `LOADING` 状態の既存のロードジョブの優先度を変更できます。

  StarRocks は v2.5 以降、Broker Load ジョブに `priority` パラメータを設定することを許可しています。

- `partial_update`

  部分更新を使用するかどうかを指定します。有効な値：`TRUE` および `FALSE`。デフォルト値：`FALSE`、この機能を無効にします。

- `partial_update_mode`

  部分更新のモードを指定します。有効な値：`row` および `column`。<ul><li>値 `row`（デフォルト）は、行モードでの部分更新を意味し、多くの列と小さなバッチでのリアルタイム更新に適しています。</li><li>値 `column` は、列モードでの部分更新を意味し、少ない列と多くの行でのバッチ更新に適しています。このようなシナリオでは、列モードを有効にすると更新速度が速くなります。たとえば、100 列のテーブルで、すべての行に対して 10 列（全体の 10%）のみが更新される場合、列モードの更新速度は 10 倍速くなります。</li></ul>

- `merge_condition`

  更新が有効になるかどうかを判断するための条件として使用したい列の名前を指定します。ソースレコードから宛先レコードへの更新は、指定された列でソースデータレコードが宛先データレコードよりも大きいか等しい値を持つ場合にのみ有効になります。
  
  > **注意**
  >
  > 指定する列は主キー列であってはなりません。また、主キーテーブルを使用するテーブルのみが条件付き更新をサポートします。

StarRocks は v3.2.3 以降、JSON データのロードをサポートしています。パラメータは次のとおりです：

- jsonpaths

  JSON データファイルからロードしたいキーの名前。マッチドモードを使用して JSON データをロードする場合にのみ、このパラメータを指定する必要があります。このパラメータの値は JSON 形式です。[Configure column mapping for JSON data loading](#configure-column-mapping-for-json-data-loading) を参照してください。

- strip_outer_array

  最外部の配列構造を削除するかどうかを指定します。有効な値：`true` および `false`。デフォルト値：`false`。
  
  実際のビジネスシナリオでは、JSON データには `[]` で示される最外部の配列構造がある場合があります。このような場合、このパラメータを `true` に設定することをお勧めします。これにより、StarRocks は最外部の角括弧 `[]` を削除し、内部の各配列を個別のデータレコードとしてロードします。このパラメータを `false` に設定すると、StarRocks は JSON データファイル全体を 1 つの配列として解析し、その配列を 1 つのデータレコードとしてロードします。たとえば、JSON データが `[ {"category" : 1, "author" : 2}, {"category" : 3, "author" : 4} ]` の場合、このパラメータを `true` に設定すると、`{"category" : 1, "author" : 2}` と `{"category" : 3, "author" : 4}` は個別のデータレコードとして解析され、個別の StarRocks テーブル行にロードされます。

- json_root

  JSON データファイルからロードしたい JSON データのルート要素。マッチドモードを使用して JSON データをロードする場合にのみ、このパラメータを指定する必要があります。このパラメータの値は有効な JsonPath 文字列です。デフォルトでは、このパラメータの値は空であり、JSON データファイルのすべてのデータがロードされることを示します。詳細については、このトピックの「[Load JSON data using matched mode with root element specified](#load-json-data-using-matched-mode-with-root-element-specified)」セクションを参照してください。

JSON データをロードする際、各 JSON オブジェクトのサイズが 4 GB を超えないように注意してください。JSON データファイル内の個々の JSON オブジェクトが 4 GB を超える場合、「This parser can't support a document that big.」というエラーが報告されます。

## 列マッピング

### CSV データロードのための列マッピングの設定

データファイルの列が StarRocks テーブルの列に順番に 1 対 1 でマッピングできる場合、データファイルと StarRocks テーブルの間の列マッピングを設定する必要はありません。

データファイルの列が StarRocks テーブルの列に順番に 1 対 1 でマッピングできない場合、`columns` パラメータを使用してデータファイルと StarRocks テーブルの間の列マッピングを設定する必要があります。これには次の 2 つのユースケースが含まれます：

- **同じ列数だが異なる列順序。また、データファイルからのデータは、StarRocks テーブル列にロードされる前に関数によって計算される必要がありません。**

  `columns` パラメータでは、データファイルの列が配置されている順序と同じ順序で StarRocks テーブル列の名前を指定する必要があります。

  たとえば、StarRocks テーブルは順番に `col1`、`col2`、`col3` の 3 つの列で構成され、データファイルも 3 つの列で構成され、StarRocks テーブルの列 `col3`、`col2`、`col1` に順番にマッピングできます。この場合、`"columns: col3, col2, col1"` と指定する必要があります。

- **異なる列数と異なる列順序。また、データファイルからのデータは、StarRocks テーブル列にロードされる前に関数によって計算される必要があります。**

  `columns` パラメータでは、データファイルの列が配置されている順序と同じ順序で StarRocks テーブル列の名前を指定し、データを計算するために使用したい関数を指定する必要があります。2 つの例は次のとおりです：

  - StarRocks テーブルは順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。データファイルは 4 つの列で構成されており、そのうち最初の 3 つの列は StarRocks テーブルの列 `col1`、`col2`、`col3` に順番にマッピングでき、4 番目の列は StarRocks テーブルのいずれの列にもマッピングできません。この場合、データファイルの 4 番目の列に一時的な名前を指定する必要があり、その一時的な名前は StarRocks テーブルの列名とは異なる必要があります。たとえば、`"columns: col1, col2, col3, temp"` と指定できます。この場合、データファイルの 4 番目の列は一時的に `temp` と名付けられます。
  - StarRocks テーブルは順番に `year`、`month`、`day` の 3 つの列で構成されています。データファイルは `yyyy-mm-dd hh:mm:ss` 形式の日付と時刻の値を含む 1 つの列のみで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"` と指定できます。この場合、`col` はデータファイル列の一時的な名前であり、`year = year(col)`、`month=month(col)`、`day=day(col)` の関数はデータファイル列 `col` からデータを抽出し、対応する StarRocks テーブル列にデータをロードします。たとえば、`year = year(col)` はデータファイル列 `col` から `yyyy` データを抽出し、StarRocks テーブル列 `year` にデータをロードするために使用されます。

詳細な例については、[Configure column mapping](#configure-column-mapping) を参照してください。

### JSON データロードのための列マッピングの設定

JSON ドキュメントのキーが StarRocks テーブルの列と同じ名前を持っている場合、シンプルモードを使用して JSON 形式のデータをロードできます。シンプルモードでは、`jsonpaths` パラメータを指定する必要はありません。このモードでは、JSON 形式のデータは中括弧 `{}` で示されるオブジェクトである必要があります。たとえば、`{"category": 1, "author": 2, "price": "3"}` です。この例では、`category`、`author`、`price` はキー名であり、これらのキーは名前によって StarRocks テーブルの列 `category`、`author`、`price` に 1 対 1 でマッピングできます。

JSON ドキュメントのキーが StarRocks テーブルの列と異なる名前を持っている場合、マッチドモードを使用して JSON 形式のデータをロードできます。マッチドモードでは、`jsonpaths` および `COLUMNS` パラメータを使用して JSON ドキュメントと StarRocks テーブルの間の列マッピングを指定する必要があります：

- `jsonpaths` パラメータでは、JSON ドキュメントに配置されている順序で JSON キーを指定します。
- `COLUMNS` パラメータでは、JSON キーと StarRocks テーブル列の間のマッピングを指定します：
  - `COLUMNS` パラメータで指定された列名は、JSON キーに順番に 1 対 1 でマッピングされます。
  - `COLUMNS` パラメータで指定された列名は、名前によって StarRocks テーブル列に 1 対 1 でマッピングされます。

マッチドモードを使用して JSON 形式のデータをロードする例については、[Load JSON data using matched mode](#load-json-data-using-matched-mode) を参照してください。

## 関連する構成項目

FE 構成項目 `max_broker_load_job_concurrency` は、StarRocks クラスター内で同時に実行できる Broker Load ジョブの最大数を指定します。

StarRocks v2.4 以前では、特定の期間内に送信された Broker Load ジョブの総数が最大数を超える場合、過剰なジョブは送信時間に基づいてキューに入れられ、スケジュールされます。

StarRocks v2.5 以降では、特定の期間内に送信された Broker Load ジョブの総数が最大数を超える場合、過剰なジョブは優先度に基づいてキューに入れられ、スケジュールされます。上記の `priority` パラメータを使用してジョブの優先度を指定できます。[ALTER LOAD](ALTER_LOAD.md) を使用して、`QUEUEING` または `LOADING` 状態の既存のジョブの優先度を変更できます。

## ジョブの分割と同時実行

Broker Load ジョブは、1 つ以上のタスクに分割され、同時に実行されることがあります。ロードジョブ内のタスクは単一のトランザクション内で実行されます。それらはすべて成功するか失敗する必要があります。StarRocks は、`LOAD` ステートメントで `data_desc` を宣言する方法に基づいて各ロードジョブを分割します：

- 複数の `data_desc` パラメータを宣言し、それぞれが異なるテーブルを指定する場合、各テーブルのデータをロードするためのタスクが生成されます。

- 複数の `data_desc` パラメータを宣言し、それぞれが同じテーブルの異なるパーティションを指定する場合、各パーティションのデータをロードするためのタスクが生成されます。

さらに、各タスクは 1 つ以上のインスタンスにさらに分割され、StarRocks クラスターの BEs または CNs に均等に分散され、同時に実行されます。StarRocks は、FE パラメータ `min_bytes_per_broker_scanner` と BE または CN ノードの数に基づいて各タスクを分割します。個々のタスクのインスタンス数を計算するための式は次のとおりです：

**個々のタスクのインスタンス数 = min(個々のタスクによってロードされるデータ量/`min_bytes_per_broker_scanner`, BE/CN ノードの数)**

ほとんどの場合、各ロードジョブには 1 つの `data_desc` のみが宣言され、各ロードジョブは 1 つのタスクにのみ分割され、そのタスクは BE または CN ノードの数と同じ数のインスタンスに分割されます。

## 例

このセクションでは、HDFS を例にとり、さまざまなロード構成を説明します。

### CSV データのロード

このセクションでは、CSV を例にとり、多様なロード要件を満たすために使用できるさまざまなパラメータ構成を説明します。

#### タイムアウト期間の設定

StarRocks データベース `test_db` には、`table1` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example1.csv` も 3 つの列で構成されており、`table1` の `col1`、`col2`、`col3` に順番にマッピングされています。

`example1.csv` から `table1` に 3600 秒以内にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label1
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example1.csv")
    INTO TABLE table1
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

#### 最大エラー許容度の設定

StarRocks データベース `test_db` には、`table2` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example2.csv` も 3 つの列で構成されており、`table2` の `col1`、`col2`、`col3` に順番にマッピングされています。

`example2.csv` から `table2` に最大エラー許容度 `0.1` でデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label2
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example2.csv")
    INTO TABLE table2

)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "max_filter_ratio" = "0.1"
);
```

#### ファイルパスからすべてのデータファイルをロード

StarRocks データベース `test_db` には、`table3` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

HDFS クラスターの `/user/starrocks/data/input/` パスに保存されているすべてのデータファイルも、それぞれ 3 つの列で構成されており、`table3` の `col1`、`col2`、`col3` に順番にマッピングされています。これらのデータファイルで使用されている列区切り文字は `\x01` です。

HDFS サーバーの `/user/starrocks/data/input/` パスに保存されているすべてのデータファイルから `table3` にデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label3
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/*")
    INTO TABLE table3
    COLUMNS TERMINATED BY "\\x01"
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

#### NameNode HA メカニズムの設定

StarRocks データベース `test_db` には、`table4` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example4.csv` も 3 つの列で構成されており、`table4` の `col1`、`col2`、`col3` にマッピングされています。

NameNode に HA メカニズムが構成された状態で `example4.csv` から `table4` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label4
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example4.csv")
    INTO TABLE table4
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>",
    "dfs.nameservices" = "my_ha",
    "dfs.ha.namenodes.my_ha" = "my_namenode1, my_namenode2","dfs.namenode.rpc-address.my_ha.my_namenode1" = "nn1_host:rpc_port",
    "dfs.namenode.rpc-address.my_ha.my_namenode2" = "nn2_host:rpc_port",
    "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
);
```

#### Kerberos 認証の設定

StarRocks データベース `test_db` には、`table5` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example5.csv` も 3 つの列で構成されており、`table5` の `col1`、`col2`、`col3` に順番にマッピングされています。

Kerberos 認証が構成され、キータブファイルパスが指定された状態で `example5.csv` から `table5` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label5
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example5.csv")
    INTO TABLE table5
    COLUMNS TERMINATED BY "\t"
)
WITH BROKER
(
    "hadoop.security.authentication" = "kerberos",
    "kerberos_principal" = "starrocks@YOUR.COM",
    "kerberos_keytab" = "/home/starRocks/starRocks.keytab"
);
```

#### データロードの取り消し

StarRocks データベース `test_db` には、`table6` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example6.csv` も 3 つの列で構成されており、`table6` の `col1`、`col2`、`col3` に順番にマッピングされています。

Broker Load ジョブを実行して `example6.csv` から `table6` にすべてのデータをロードしました。

ロードしたデータを取り消したい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label6
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example6.csv")
    NEGATIVE
    INTO TABLE table6
    COLUMNS TERMINATED BY "\t"
)
WITH BROKER
(
    "hadoop.security.authentication" = "kerberos",
    "kerberos_principal" = "starrocks@YOUR.COM",
    "kerberos_keytab" = "/home/starRocks/starRocks.keytab"
);
```

#### 宛先パーティションの指定

StarRocks データベース `test_db` には、`table7` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example7.csv` も 3 つの列で構成されており、`table7` の `col1`、`col2`、`col3` に順番にマッピングされています。

`example7.csv` から `table7` の 2 つのパーティション `p1` と `p2` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label7
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example7.csv")
    INTO TABLE table7
    PARTITION (p1, p2)
    COLUMNS TERMINATED BY ","
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

#### 列マッピングの設定

StarRocks データベース `test_db` には、`table8` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example8.csv` も 3 つの列で構成されており、`table8` の `col2`、`col1`、`col3` に順番にマッピングされています。

`example8.csv` から `table8` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label8
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example8.csv")
    INTO TABLE table8
    COLUMNS TERMINATED BY ","
    (col2, col1, col3)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

> **注意**
>
> 上記の例では、`example8.csv` の列は `table8` の列と同じ順序でマッピングできません。そのため、`column_list` を使用して `example8.csv` と `table8` の間の列マッピングを設定する必要があります。

#### フィルタ条件の設定

StarRocks データベース `test_db` には、`table9` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example9.csv` も 3 つの列で構成されており、`table9` の `col1`、`col2`、`col3` に順番にマッピングされています。

`example9.csv` の最初の列の値が `20180601` より大きいデータレコードのみを `table9` にロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label9
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example9.csv")
    INTO TABLE table9
    (col1, col2, col3)
    where col1 > 20180601
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

> **注意**
>
> 上記の例では、`example9.csv` の列は `table9` の列と同じ順序でマッピングできますが、WHERE 句を使用して列ベースのフィルタ条件を指定する必要があります。そのため、`column_list` を使用して `example9.csv` と `table9` の間の列マッピングを設定する必要があります。

#### HLL 型の列を含むテーブルへのデータロード

StarRocks データベース `test_db` には、`table10` という名前のテーブルがあります。このテーブルは、順番に `id`、`col1`、`col2`、`col3` の 4 つの列で構成されています。`col1` と `col2` は HLL 型の列として定義されています。

データファイル `example10.csv` は 3 つの列で構成されており、そのうち最初の列は `table10` の `id` にマッピングされ、2 番目と 3 番目の列は `table10` の `col1` と `col2` に順番にマッピングされています。`example10.csv` の 2 番目と 3 番目の列の値は、関数を使用して HLL 型のデータに変換され、`table10` の `col1` と `col2` にロードされます。

`example10.csv` から `table10` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label10
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example10.csv")
    INTO TABLE table10
    COLUMNS TERMINATED BY ","
    (id, temp1, temp2)
    SET
    (
        col1 = hll_hash(temp1),
        col2 = hll_hash(temp2),
        col3 = empty_hll()
     )
 )
 WITH BROKER
 (
     "username" = "<hdfs_username>",
     "password" = "<hdfs_password>"
 );
```

> **注意**
>
> 上記の例では、`example10.csv` の 3 つの列は `column_list` を使用して順番に `id`、`temp1`、`temp2` と名付けられます。その後、関数を使用してデータを次のように変換します：
>
> - `hll_hash` 関数を使用して、`example10.csv` の `temp1` と `temp2` の値を HLL 型のデータに変換し、`example10.csv` の `temp1` と `temp2` を `table10` の `col1` と `col2` にマッピングします。
>
> - `hll_empty` 関数を使用して、指定されたデフォルト値を `table10` の `col3` に埋め込みます。

関数 `hll_hash` と `hll_empty` の使用法については、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md) および [hll_empty](../../sql-functions/scalar-functions/hll_empty.md) を参照してください。

#### ファイルパスからパーティションフィールドの値を抽出

Broker Load は、宛先 StarRocks テーブルの列定義に基づいて、ファイルパスに含まれる特定のパーティションフィールドの値を解析することをサポートしています。この StarRocks の機能は、Apache Spark™ の Partition Discovery 機能に似ています。

StarRocks データベース `test_db` には、`table11` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3`、`city`、`utc_date` の 5 つの列で構成されています。

HDFS クラスターのファイルパス `/user/starrocks/data/input/dir/city=beijing` には、次のデータファイルが含まれています：

- `/user/starrocks/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv`

- `/user/starrocks/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv`

これらのデータファイルはそれぞれ 3 つの列で構成されており、`table11` の `col1`、`col2`、`col3` に順番にマッピングされています。

ファイルパス `/user/starrocks/data/input/dir/city=beijing/utc_date=*/*` からすべてのデータファイルを `table11` にロードし、同時にファイルパスに含まれるパーティションフィールド `city` と `utc_date` の値を抽出して `table11` の `city` と `utc_date` にロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label11
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/dir/city=beijing/*/*")
    INTO TABLE table11
    FORMAT AS "csv"
    (col1, col2, col3)
    COLUMNS FROM PATH AS (city, utc_date)
    SET (uniq_id = md5sum(k1, city))
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

#### `%3A` を含むファイルパスからパーティションフィールドの値を抽出

HDFS では、ファイルパスにコロン (:) を含めることはできません。すべてのコロン (:) は `%3A` に変換されます。

StarRocks データベース `test_db` には、`table12` という名前のテーブルがあります。このテーブルは、順番に `data_time`、`col1`、`col2` の 3 つの列で構成されています。テーブルスキーマは次のとおりです：

```SQL
data_time DATETIME,
col1        INT,
col2        INT
```

HDFS クラスターのファイルパス `/user/starrocks/data` には、次のデータファイルが含まれています：

- `/user/starrocks/data/data_time=2020-02-17 00%3A00%3A00/example12.csv`

- `/user/starrocks/data/data_time=2020-02-18 00%3A00%3A00/example12.csv`

`example12.csv` から `table12` にすべてのデータをロードし、同時にファイルパスからパーティションフィールド `data_time` の値を抽出して `table12` の `data_time` にロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label12
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/*/example12.csv")
    INTO TABLE table12
    COLUMNS TERMINATED BY ","
    FORMAT AS "csv"
    (col1,col2)
    COLUMNS FROM PATH AS (data_time)
    SET (data_time = str_to_date(data_time, '%Y-%m-%d %H%%3A%i%%3A%s'))
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

> **注意**
>
> 上記の例では、パーティションフィールド `data_time` から抽出された値は `%3A` を含む文字列であり、たとえば `2020-02-17 00%3A00%3A00` です。そのため、`str_to_date` 関数を使用して文字列を DATETIME 型のデータに変換し、`table8` の `data_time` にロードする必要があります。

#### `format_type_options` の設定

StarRocks データベース `test_db` には、`table13` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example13.csv` も 3 つの列で構成されており、`table13` の `col2`、`col1`、`col3` に順番にマッピングされています。

`example13.csv` から `table13` にすべてのデータをロードし、`example13.csv` の最初の 2 行をスキップし、列区切り文字の前後のスペースを削除し、`enclose` を `\` に設定し、`escape` を `\` に設定したい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label13
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/*/example13.csv")
    INTO TABLE table13
    COLUMNS TERMINATED BY ","
    FORMAT AS "CSV"
    (
        skip_header = 2
        trim_space = TRUE
        enclose = "\""
        escape = "\\"
    )
    (col2, col1, col3)
)
WITH BROKER
(
    "username" = "hdfs_username",
    "password" = "hdfs_password"
)
PROPERTIES
(
    "timeout" = "3600"
);
```

### Parquet データのロード

このセクションでは、Parquet データをロードする際に注意すべきパラメータ設定について説明します。

StarRocks データベース `test_db` には、`table13` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example13.parquet` も 3 つの列で構成されており、`table13` の `col1`、`col2`、`col3` に順番にマッピングされています。

`example13.parquet` から `table13` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label13
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example13.parquet")
    INTO TABLE table13
    FORMAT AS "parquet"
    (col1, col2, col3)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

> **注意**
>
> デフォルトでは、Parquet データをロードする際、StarRocks はファイル名に **.parquet** 拡張子が含まれているかどうかに基づいてデータファイル形式を決定します。ファイル名に **.parquet** 拡張子が含まれていない場合、`FORMAT AS` を使用してデータファイル形式を `Parquet` として指定する必要があります。

### ORC データのロード

このセクションでは、ORC データをロードする際に注意すべきパラメータ設定について説明します。

StarRocks データベース `test_db` には、`table14` という名前のテーブルがあります。このテーブルは、順番に `col1`、`col2`、`col3` の 3 つの列で構成されています。

データファイル `example14.orc` も 3 つの列で構成されており、`table14` の `col1`、`col2`、`col3` に順番にマッピングされています。

`example14.orc` から `table14` にすべてのデータをロードしたい場合、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label14
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example14.orc")
    INTO TABLE table14
    FORMAT AS "orc"
    (col1, col2, col3)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

> **注意**
>
> - デフォルトでは、ORC データをロードする際、StarRocks はファイル名に **.orc** 拡張子が含まれているかどうかに基づいてデータファイル形式を決定します。ファイル名に **.orc** 拡張子が含まれていない場合、`FORMAT AS` を使用してデータファイル形式を `ORC` として指定する必要があります。
>
> - StarRocks v2.3 以前では、データファイルに ARRAY 型の列が含まれている場合、ORC データファイルの列が StarRocks テーブルの対応する列と同じ名前であることを確認し、SET 句で列を指定できないことを確認する必要があります。

### JSON データのロード

このセクションでは、JSON データをロードする際に注意すべきパラメータ設定について説明します。

StarRocks データベース `test_db` には、次のスキーマを持つ `tbl1` という名前のテーブルがあります：

```SQL
`category` varchar(512) NULL COMMENT "",
`author` varchar(512) NULL COMMENT "",
`title` varchar(512) NULL COMMENT "",
`price` double NULL COMMENT ""
```

#### シンプルモードを使用した JSON データのロード

データファイル `example1.json` が次のデータで構成されていると仮定します：

```JSON
{"category":"C++","author":"avc","title":"C++ primer","price":895}
```

`example1.json` から `tbl1` にすべてのデータをロードするには、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label15
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example1.csv")
    INTO TABLE tbl1
    FORMAT AS "json"
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
);
```

> **注意**
>
> 上記の例では、`columns` および `jsonpaths` パラメータは指定されていません。そのため、`example1.json` のキーは `tbl1` の列に名前によってマッピングされます。

#### マッチドモードを使用した JSON データのロード

StarRocks は、次の手順で JSON データをマッチングして処理します：

1. （オプション）`strip_outer_array` パラメータ設定によって指示されたように、最外部の配列構造を削除します。

   > **注意**
   >
   > この手順は、JSON データの最外層が `[]` で示される配列構造である場合にのみ実行されます。`strip_outer_array` を `true` に設定する必要があります。

2. （オプション）`json_root` パラメータ設定によって指示されたように、JSON データのルート要素をマッチングします。

   > **注意**
   >
   > この手順は、JSON データにルート要素がある場合にのみ実行されます。`json_root` パラメータを使用してルート要素を指定する必要があります。

3. `jsonpaths` パラメータ設定によって指示されたように、指定された JSON データを抽出します。

##### ルート要素を指定せずにマッチドモードを使用して JSON データをロード

データファイル `example2.json` が次のデータで構成されていると仮定します：

```JSON
[
    {"category":"xuxb111","author":"1avc","title":"SayingsoftheCentury","price":895},
    {"category":"xuxb222","author":"2avc","title":"SayingsoftheCentury","price":895},
    {"category":"xuxb333","author":"3avc","title":"SayingsoftheCentury","price":895}
]
```

`example2.json` から `category`、`author`、`price` のみをロードするには、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label16
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example2.csv")
    INTO TABLE tbl1
    FORMAT AS "json"
    (category, price, author)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "strip_outer_array" = "true",
    "jsonpaths" = "[\"$.category\",\"$.price\",\"$.author\"]"
);
```

> **注意**
>
> 上記の例では、JSON データの最外層が `[]` で示される配列構造です。配列構造は、各データレコードを表す複数の JSON オブジェクトで構成されています。そのため、最外部の配列構造を削除するために `strip_outer_array` を `true` に設定する必要があります。ロードしたくないキー **title** はロード中に無視されます。

##### ルート要素を指定してマッチドモードを使用して JSON データをロード

データファイル `example3.json` が次のデータで構成されていると仮定します：

```JSON
{
    "id": 10001,
    "RECORDS":[
        {"category":"11","title":"SayingsoftheCentury","price":895,"timestamp":1589191587},
        {"category":"22","author":"2avc","price":895,"timestamp":1589191487},
        {"category":"33","author":"3avc","title":"SayingsoftheCentury","timestamp":1589191387}
    ],
    "comments": ["3 records", "there will be 3 rows"]
}
```

`example3.json` から `category`、`author`、`price` のみをロードするには、次のコマンドを実行します：

```SQL
LOAD LABEL test_db.label17
(
    DATA INFILE("hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/example3.csv")
    INTO TABLE tbl1
    FORMAT AS "json"
    (category, price, author)
)
WITH BROKER
(
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
)
PROPERTIES
(
    "json_root"="$.RECORDS",
    "strip_outer_array" = "true",
    "jsonpaths" = "[\"$.category\",\"$.price\",\"$.author\"]"
);
```

> **注意**
>
> 上記の例では、JSON データの最外層が `[]` で示される配列構造です。配列構造は、各データレコードを表す複数の JSON オブジェクトで構成されています。そのため、最外部の配列構造を削除するために `strip_outer_array` を `true` に設定する必要があります。ロードしたくないキー `title` と `timestamp` はロード中に無視されます。また、`json_root` パラメータを使用して、JSON データのルート要素である配列を指定します。