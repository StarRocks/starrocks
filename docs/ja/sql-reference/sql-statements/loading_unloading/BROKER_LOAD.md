---
displayed_sidebar: docs
toc_max_heading_level: 5
---

# BROKER LOAD

import InsertPrivNote from '../../../_assets/commonMarkdown/insertPrivNote.md'

## 説明

StarRocksは、MySQLベースのロード方法であるBroker Loadを提供します。ロードジョブを送信すると、StarRocksは非同期でジョブを実行します。`SELECT * FROM information_schema.loads`を使用してジョブの結果をクエリできます。この機能はv3.1以降でサポートされています。背景情報、原則、サポートされているデータファイル形式、単一テーブルロードと複数テーブルロードの実行方法、ジョブ結果の表示方法については、[loading overview](../../../loading/Loading_intro.md)を参照してください。

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

StarRocksでは、いくつかのリテラルがSQL言語によって予約キーワードとして使用されます。これらのキーワードをSQL文で直接使用しないでください。SQL文でそのようなキーワードを使用したい場合は、バックティック (`) で囲んでください。[Keywords](../keywords.md)を参照してください。

## パラメータ

### database_name と label_name

`label_name`はロードジョブのラベルを指定します。

`database_name`は、宛先テーブルが属するデータベースの名前をオプションで指定します。

各ロードジョブには、データベース全体で一意のラベルがあります。ロードジョブのラベルを使用して、ロードジョブの実行ステータスを表示し、同じデータを繰り返しロードするのを防ぐことができます。ロードジョブが**FINISHED**状態に入ると、そのラベルは再利用できません。**CANCELLED**状態に入ったロードジョブのラベルのみが再利用可能です。ほとんどの場合、ロードジョブのラベルは、そのロードジョブを再試行して同じデータをロードするために再利用され、Exactly-Onceセマンティクスを実装します。

ラベルの命名規則については、[System limits](../../System_limit.md)を参照してください。

### data_desc

ロードするデータのバッチの説明です。各`data_desc`ディスクリプタは、データソース、ETL関数、宛先StarRocksテーブル、および宛先パーティションなどの情報を宣言します。

Broker Loadは、一度に複数のデータファイルをロードすることをサポートしています。1つのロードジョブで、複数の`data_desc`ディスクリプタを使用してロードしたい複数のデータファイルを宣言するか、1つの`data_desc`ディスクリプタを使用して、すべてのデータファイルをロードしたいファイルパスを宣言することができます。Broker Loadは、複数のデータファイルをロードする各ロードジョブのトランザクションの原子性も保証できます。原子性とは、1つのロードジョブで複数のデータファイルをロードする際に、すべて成功するか失敗するかのいずれかであることを意味します。いくつかのデータファイルのロードが成功し、他のファイルのロードが失敗することはありません。

`data_desc`は次の構文をサポートしています：

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

`data_desc`には次のパラメータが含まれている必要があります：

- `file_path`

  ロードしたい1つ以上のデータファイルの保存パスを指定します。

  このパラメータを1つのデータファイルの保存パスとして指定できます。たとえば、HDFSサーバー上のパス`/user/data/tablename`から`20210411`という名前のデータファイルをロードするために、このパラメータを`"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/20210411"`として指定できます。

  また、ワイルドカード`?`、`*`、`[]`、`{}`、または`^`を使用して、複数のデータファイルの保存パスとしてこのパラメータを指定することもできます。[Wildcard reference](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/fs/FileSystem.html#globStatus-org.apache.hadoop.fs.Path-)を参照してください。たとえば、このパラメータを`"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/*/*"`または`"hdfs://<hdfs_host>:<hdfs_port>/user/data/tablename/dt=202104*/*"`として指定し、HDFSサーバー上のパス`/user/data/tablename`のすべてのパーティションまたは`202104`パーティションのみからデータファイルをロードすることができます。

  > **注意**
  >
  > ワイルドカードは中間パスを指定するためにも使用できます。

  前述の例では、`hdfs_host`および`hdfs_port`パラメータは次のように説明されています：

  - `hdfs_host`: HDFSクラスター内のNameNodeホストのIPアドレス。

  - `hdfs_port`: HDFSクラスター内のNameNodeホストのFSポート。デフォルトのポート番号は`9000`です。

  > **注意**
  >
  > - Broker Loadは、S3またはS3Aプロトコルに従ってAWS S3へのアクセスをサポートしています。したがって、AWS S3からデータをロードする場合、ファイルパスとして渡すS3 URIのプレフィックスに`s3://`または`s3a://`を含めることができます。
  > - Broker Loadは、gsプロトコルに従ってのみGoogle GCSへのアクセスをサポートしています。したがって、Google GCSからデータをロードする場合、ファイルパスとして渡すGCS URIのプレフィックスに`gs://`を含める必要があります。
  > - Blob Storageからデータをロードする場合、wasbまたはwasbsプロトコルを使用してデータにアクセスする必要があります：
  >   - ストレージアカウントがHTTP経由でのアクセスを許可する場合、wasbプロトコルを使用し、ファイルパスを`wasb://<container_name>@<storage_account_name>.blob.core.windows.net/<path>/<file_name>/*`として記述します。
  >   - ストレージアカウントがHTTPS経由でのアクセスを許可する場合、wasbsプロトコルを使用し、ファイルパスを`wasbs://<container_name>@<storage_account_name>.blob.core.windows.net/<path>/<file_name>/*`として記述します。
  > - Data Lake Storage Gen2からデータをロードする場合、abfsまたはabfssプロトコルを使用してデータにアクセスする必要があります：
  >   - ストレージアカウントがHTTP経由でのアクセスを許可する場合、abfsプロトコルを使用し、ファイルパスを`abfs://<container_name>@<storage_account_name>.dfs.core.windows.net/<file_name>`として記述します。
  >   - ストレージアカウントがHTTPS経由でのアクセスを許可する場合、abfssプロトコルを使用し、ファイルパスを`abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<file_name>`として記述します。
  > - Data Lake Storage Gen1からデータをロードする場合、adlプロトコルを使用してデータにアクセスし、ファイルパスを`adl://<data_lake_storage_gen1_name>.azuredatalakestore.net/<path>/<file_name>`として記述します。

- `INTO TABLE`

  宛先StarRocksテーブルの名前を指定します。

`data_desc`には次のパラメータもオプションで含めることができます：

- `NEGATIVE`

  特定のデータバッチのロードを取り消します。これを達成するには、`NEGATIVE`キーワードを指定して同じデータバッチをロードする必要があります。

  > **注意**
  >
  > このパラメータは、StarRocksテーブルが集計テーブルであり、そのすべての値列が`sum`関数によって計算される場合にのみ有効です。

- `PARTITION`

   データをロードしたいパーティションを指定します。デフォルトでは、このパラメータを指定しない場合、ソースデータはStarRocksテーブルのすべてのパーティションにロードされます。

- `TEMPORARY PARTITION`

  データをロードしたい[一時パーティション](../../../table_design/data_distribution/Temporary_partition.md)の名前を指定します。複数の一時パーティションを指定することができ、カンマ（,）で区切る必要があります。

- `COLUMNS TERMINATED BY`

  データファイルで使用される列区切り文字を指定します。デフォルトでは、このパラメータを指定しない場合、このパラメータは`\t`（タブ）にデフォルト設定されます。このパラメータを使用して指定した列区切り文字は、実際にデータファイルで使用されている列区切り文字と同じである必要があります。そうでない場合、データ品質が不十分なためロードジョブが失敗し、その`State`は`CANCELLED`になります。

  Broker LoadジョブはMySQLプロトコルに従って送信されます。StarRocksとMySQLはどちらもロードリクエストで文字をエスケープします。したがって、列区切り文字がタブのような不可視文字である場合、列区切り文字の前にバックスラッシュ（\）を追加する必要があります。たとえば、列区切り文字が`\t`の場合は`\\t`と入力する必要があり、列区切り文字が`\n`の場合は`\\n`と入力する必要があります。Apache Hive™ファイルは`\x01`を列区切り文字として使用するため、データファイルがHiveからのものである場合は`\\x01`と入力する必要があります。

  > **注意**
  >
  > - CSVデータの場合、カンマ（,）、タブ、またはパイプ（|）のようなUTF-8文字列をテキストデリミタとして使用できますが、その長さは50バイトを超えてはなりません。
  > - Null値は`\N`を使用して示されます。たとえば、データファイルが3つの列で構成され、そのデータファイルのレコードが最初と3番目の列にデータを保持し、2番目の列にデータがない場合、この状況では2番目の列にNull値を示すために`\N`を使用する必要があります。これは、レコードを`a,\N,b`としてコンパイルする必要があることを意味し、`a,,b`ではありません。`a,,b`は、レコードの2番目の列が空の文字列を保持していることを示します。

- `ROWS TERMINATED BY`

  データファイルで使用される行区切り文字を指定します。デフォルトでは、このパラメータを指定しない場合、このパラメータは`\n`（改行）にデフォルト設定されます。このパラメータを使用して指定した行区切り文字は、実際にデータファイルで使用されている行区切り文字と同じである必要があります。そうでない場合、データ品質が不十分なためロードジョブが失敗し、その`State`は`CANCELLED`になります。このパラメータはv2.5.4以降でサポートされています。

  行区切り文字の使用に関する注意事項については、前述の`COLUMNS TERMINATED BY`パラメータの使用に関する注意事項を参照してください。

- `FORMAT AS`

  データファイルの形式を指定します。有効な値：`CSV`、`Parquet`、および`ORC`。デフォルトでは、このパラメータを指定しない場合、StarRocksは`file_path`パラメータで指定されたファイル名拡張子**.csv**、**.parquet**、または**.orc**に基づいてデータファイル形式を決定します。

- `format_type_options`

  `FORMAT AS`が`CSV`に設定されている場合のCSV形式オプションを指定します。構文：

  ```JSON
  (
      key = value
      key = value
      ...
  )
  ```

  > **注意**
  >
  > `format_type_options`はv3.0以降でサポートされています。

  次の表はオプションを説明しています。

| **Parameter** | **Description**                                              |
| ------------- | ------------------------------------------------------------ |
| skip_header   | CSV形式のデータファイルの最初の行をスキップするかどうかを指定します。タイプ: INTEGER。デフォルト値: `0`。<br />一部のCSV形式のデータファイルでは、最初の行はメタデータ（列名や列データ型など）を定義するために使用されます。`skip_header`パラメータを設定することで、StarRocksがデータロード中にデータファイルの最初の行をスキップできるようになります。たとえば、このパラメータを`1`に設定すると、StarRocksはデータロード中にデータファイルの最初の行をスキップします。<br />データファイルの最初の行は、ロード文で指定した行区切り文字を使用して区切られている必要があります。 |
| trim_space    | CSV形式のデータファイルから列区切り文字の前後のスペースを削除するかどうかを指定します。タイプ: BOOLEAN。デフォルト値: `false`。<br />一部のデータベースでは、データをCSV形式のデータファイルとしてエクスポートする際に列区切り文字にスペースが追加されます。これらのスペースは、先行スペースまたは後続スペースと呼ばれます。`trim_space`パラメータを設定することで、StarRocksがデータロード中にこれらの不要なスペースを削除できるようになります。<br />StarRocksは、`enclose`で指定された文字で囲まれたフィールド内のスペース（先行スペースおよび後続スペースを含む）を削除しません。たとえば、次のフィールド値は、パイプ（<code class="language-text">&#124;</code>）を列区切り文字として使用し、二重引用符（`"`)を`enclose`で指定された文字として使用しています：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124; "Love StarRocks" &#124;</code> <br />`trim_space`を`true`に設定すると、StarRocksは前述のフィールド値を次のように処理します：<br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> <br /><code class="language-text">&#124;" Love StarRocks "&#124;</code> <br /><code class="language-text">&#124;"Love StarRocks"&#124;</code> |
| enclose       | CSV形式のデータファイルのフィールド値を[RFC4180](https://www.rfc-editor.org/rfc/rfc4180)に従ってラップするために使用される文字を指定します。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字は単一引用符（`'`）および二重引用符（`"`）です。<br />`enclose`で指定された文字で囲まれたすべての特殊文字（行区切り文字や列区切り文字を含む）は通常の記号と見なされます。StarRocksは、`enclose`で指定された文字として任意の単一バイト文字を指定できるため、RFC4180よりも多くのことができます。<br />フィールド値に`enclose`で指定された文字が含まれている場合、同じ文字を使用してその`enclose`で指定された文字をエスケープできます。たとえば、`enclose`を`"`に設定し、フィールド値が`a "quoted" c`の場合、このフィールド値をデータファイルに`"a ""quoted"" c"`として入力できます。 |
| escape        | 行区切り文字、列区切り文字、エスケープ文字、`enclose`で指定された文字などのさまざまな特殊文字をエスケープするために使用される文字を指定します。これらの文字は、StarRocksによって通常の文字と見なされ、フィールド値の一部として解析されます。タイプ: 単一バイト文字。デフォルト値: `NONE`。最も一般的な文字はスラッシュ（`\`）であり、SQL文では二重スラッシュ（`\\`）として記述する必要があります。<br />**注意**<br />`escape`で指定された文字は、各ペアの`enclose`で指定された文字の内側と外側の両方に適用されます。<br />次の2つの例があります：<ul><li>`enclose`を`"`に設定し、`escape`を`\`に設定すると、StarRocksは`"say \"Hello world\""`を`say "Hello world"`に解析します。</li><li>列区切り文字がカンマ（`,`）であると仮定します。`escape`を`\`に設定すると、StarRocksは`a, b\, c`を2つの別々のフィールド値に解析します：`a`と`b, c`。</li></ul> |

- `column_list`

  データファイルとStarRocksテーブル間の列マッピングを指定します。構文: `(<column_name>[, <column_name> ...])`。`column_list`で宣言された列は、名前によってStarRocksテーブルの列にマッピングされます。

  > **注意**
  >
  > データファイルの列がStarRocksテーブルの列に順番にマッピングされている場合、`column_list`を指定する必要はありません。

  データファイルの特定の列をスキップしたい場合、その列を一時的にStarRocksテーブルの列名とは異なる名前にするだけで済みます。詳細については、[loading overview](../../../loading/Loading_intro.md)を参照してください。

- `COLUMNS FROM PATH AS`

  指定したファイルパスから1つ以上のパーティションフィールドに関する情報を抽出します。このパラメータは、ファイルパスにパーティションフィールドが含まれている場合にのみ有効です。

  たとえば、データファイルが`/path/col_name=col_value/file1`というパスに保存されており、`col_name`がパーティションフィールドであり、StarRocksテーブルの列にマッピングできる場合、このパラメータを`col_name`として指定できます。このようにして、StarRocksはパスから`col_value`値を抽出し、それらを`col_name`がマッピングされているStarRocksテーブルの列にロードします。

  > **注意**
  >
  > このパラメータは、HDFSからデータをロードする場合にのみ利用可能です。

- `SET`

  データファイルの列を変換するために使用したい1つ以上の関数を指定します。例：

  - StarRocksテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。データファイルは4つの列で構成されており、そのうち最初の2つの列はStarRocksテーブルの`col1`と`col2`に順番にマッピングされ、最後の2つの列の合計はStarRocksテーブルの`col3`にマッピングされます。この場合、`column_list`を`(col1,col2,tmp_col3,tmp_col4)`として指定し、SET句で`(col3=tmp_col3+tmp_col4)`を指定してデータ変換を実装する必要があります。
  - StarRocksテーブルは、順番に`year`、`month`、`day`の3つの列で構成されています。データファイルは、`yyyy-mm-dd hh:mm:ss`形式の日付と時刻の値を含む1つの列のみで構成されています。この場合、`column_list`を`(tmp_time)`として指定し、SET句で`(year = year(tmp_time), month=month(tmp_time), day=day(tmp_time))`を指定してデータ変換を実装する必要があります。

- `WHERE`

  ソースデータをフィルタリングするための条件を指定します。StarRocksは、WHERE句で指定されたフィルタ条件を満たすソースデータのみをロードします。

### WITH BROKER

v2.3以前では、使用したいブローカーを指定するために`WITH BROKER "<broker_name>"`を入力します。v2.5以降では、ブローカーを指定する必要はありませんが、`WITH BROKER`キーワードを保持する必要があります。

> **注意**
>
> v2.4以前では、StarRocksはBroker Loadジョブを実行する際に、StarRocksクラスターと外部ストレージシステム間の接続を確立するためにブローカーに依存していました。これは「ブローカーを使用したロード」と呼ばれます。ブローカーは、ファイルシステムインターフェースと統合された独立したステートレスサービスです。ブローカーを使用すると、StarRocksは外部ストレージシステムに保存されているデータファイルにアクセスして読み取ることができ、独自のコンピューティングリソースを使用してこれらのデータファイルのデータを事前処理してロードできます。
>
> v2.5以降、StarRocksはブローカーへの依存を排除し、「ブローカーを使用しないロード」を実装しました。
>
> StarRocksクラスターにデプロイされたブローカーを確認するには、[SHOW BROKER](../cluster-management/nodes_processes/SHOW_BROKER.md)文を使用できます。ブローカーがデプロイされていない場合は、[Deploy a broker](../../../deployment/deploy_broker.md)に記載された手順に従ってブローカーをデプロイできます。

### StorageCredentialParams

StarRocksがストレージシステムにアクセスするために使用する認証情報。

#### HDFS

オープンソースのHDFSは、シンプル認証とKerberos認証の2つの認証方法をサポートしています。Broker Loadはデフォルトでシンプル認証を使用します。オープンソースのHDFSは、NameNodeのHAメカニズムの設定もサポートしています。ストレージシステムとしてオープンソースのHDFSを選択した場合、認証設定とHA設定を次のように指定できます：

- 認証設定

  - シンプル認証を使用する場合、`StorageCredentialParams`を次のように設定します：

    ```Plain
    "hadoop.security.authentication" = "simple",
    "username" = "<hdfs_username>",
    "password" = "<hdfs_password>"
    ```

    `StorageCredentialParams`のパラメータは次のように説明されています。

    | パラメータ                       | 説明                                                  |
    | ------------------------------- | ------------------------------------------------------------ |
    | hadoop.security.authentication  | 認証方法。有効な値：`simple`および`kerberos`。デフォルト値：`simple`。`simple`はシンプル認証を表し、認証なしを意味し、`kerberos`はKerberos認証を表します。 |
    | username                        | HDFSクラスターのNameNodeにアクセスするために使用するアカウントのユーザー名。 |
    | password                        | HDFSクラスターのNameNodeにアクセスするために使用するアカウントのパスワード。 |

  - Kerberos認証を使用する場合、`StorageCredentialParams`を次のように設定します：

    ```Plain
    "hadoop.security.authentication" = "kerberos",
    "kerberos_principal" = "nn/zelda1@ZELDA.COM",
    "kerberos_keytab" = "/keytab/hive.keytab",
    "kerberos_keytab_content" = "YWFhYWFh"
    ```

    `StorageCredentialParams`のパラメータは次のように説明されています。

    | パラメータ                       | 説明                                                  |
    | ------------------------------- | ------------------------------------------------------------ |
    | hadoop.security.authentication  | 認証方法。有効な値：`simple`および`kerberos`。デフォルト値：`simple`。`simple`はシンプル認証を表し、認証なしを意味し、`kerberos`はKerberos認証を表します。 |
    | kerberos_principal              | 認証されるKerberosプリンシパル。各プリンシパルは、HDFSクラスター全体で一意であることを保証するために、次の3つの部分で構成されます：<ul><li>`username`または`servicename`: プリンシパルの名前。</li><li>`instance`: HDFSクラスター内で認証されるノードをホストするサーバーの名前。サーバー名は、HDFSクラスターが独立して認証される複数のDataNodeで構成されている場合に、プリンシパルが一意であることを保証するのに役立ちます。</li><li>`realm`: レルムの名前。レルム名は大文字でなければなりません。</li></ul>例：`nn/zelda1@ZELDA.COM`。 |
    | kerberos_keytab                 | Kerberosキータブファイルの保存パス。 |
    | kerberos_keytab_content         | KerberosキータブファイルのBase64エンコードされた内容。`kerberos_keytab`または`kerberos_keytab_content`のいずれかを指定できます。 |

    複数のKerberosユーザーを設定した場合、少なくとも1つの独立した[broker group](../../../deployment/deploy_broker.md)がデプロイされていることを確認し、ロード文で`WITH BROKER "<broker_name>"`を入力して使用したいブローカーグループを指定する必要があります。さらに、ブローカーの起動スクリプトファイル**start_broker.sh**を開き、ファイルの42行目を変更して、ブローカーが**krb5.conf**ファイルを読み取れるようにする必要があります。例：

    ```Plain
    export JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx1024m -Dfile.encoding=UTF-8 -Djava.security.krb5.conf=/etc/krb5.conf"
    ```

    > **注意**
    >
    > - 前述の例では、`/etc/krb5.conf`は実際の**krb5.conf**ファイルの保存パスに置き換えることができます。ブローカーがそのファイルを読み取る権限を持っていることを確認してください。ブローカーグループが複数のブローカーで構成されている場合、各ブローカーノードで**start_broker.sh**ファイルを変更し、変更を有効にするためにブローカーノードを再起動する必要があります。
    > - StarRocksクラスターにデプロイされたブローカーを確認するには、[SHOW BROKER](../cluster-management/nodes_processes/SHOW_BROKER.md)文を使用できます。

- HA設定

  HDFSクラスターのNameNodeにHAメカニズムを設定できます。これにより、NameNodeが別のノードに切り替えられた場合でも、StarRocksは自動的に新しいNameNodeを識別できます。これには次のシナリオが含まれます：

  - 1つのKerberosユーザーが設定された単一のHDFSクラスターからデータをロードする場合、ブローカーを使用したロードとブローカーを使用しないロードの両方がサポートされています。

    - ブローカーを使用したロードを実行するには、少なくとも1つの独立した[broker group](../../../deployment/deploy_broker.md)がデプロイされていることを確認し、HDFSクラスターを提供するブローカーノードの`{deploy}/conf`パスに`hdfs-site.xml`ファイルを配置します。StarRocksはブローカーの起動時に`{deploy}/conf`パスを環境変数`CLASSPATH`に追加し、ブローカーがHDFSクラスターのノード情報を読み取れるようにします。

    - ブローカーを使用しないロードを実行するには、クラスター内のすべてのFE、BE、およびCNノードのデプロイメントディレクトリの`conf/core-site.xml`に`hadoop.security.authentication = kerberos`を設定し、`kinit`コマンドを使用してKerberosアカウントを設定するだけです。

  - 複数のKerberosユーザーが設定された単一のHDFSクラスターからデータをロードする場合、ブローカーを使用したロードのみがサポートされています。少なくとも1つの独立した[broker group](../../../deployment/deploy_broker.md)がデプロイされていることを確認し、HDFSクラスターを提供するブローカーノードの`{deploy}/conf`パスに`hdfs-site.xml`ファイルを配置します。StarRocksはブローカーの起動時に`{deploy}/conf`パスを環境変数`CLASSPATH`に追加し、ブローカーがHDFSクラスターのノード情報を読み取れるようにします。

  - 複数のHDFSクラスターからデータをロードする場合（1つまたは複数のKerberosユーザーが設定されているかどうかに関係なく）、ブローカーを使用したロードのみがサポートされています。これらのHDFSクラスターのそれぞれに対して少なくとも1つの独立した[broker group](../../../deployment/deploy_broker.md)がデプロイされていることを確認し、ブローカーがHDFSクラスターのノード情報を読み取れるようにするために次のいずれかのアクションを実行します：

    - HDFSクラスターを提供するブローカーノードの`{deploy}/conf`パスに`hdfs-site.xml`ファイルを配置します。StarRocksはブローカーの起動時に`{deploy}/conf`パスを環境変数`CLASSPATH`に追加し、そのHDFSクラスターのノード情報をブローカーが読み取れるようにします。

    - ジョブ作成時に次のHA設定を追加します：

      ```Plain
      "dfs.nameservices" = "ha_cluster",
      "dfs.ha.namenodes.ha_cluster" = "ha_n1,ha_n2",
      "dfs.namenode.rpc-address.ha_cluster.ha_n1" = "<hdfs_host>:<hdfs_port>",
      "dfs.namenode.rpc-address.ha_cluster.ha_n2" = "<hdfs_host>:<hdfs_port>",
      "dfs.client.failover.proxy.provider" = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      ```

      HA設定のパラメータは次のように説明されています。

      | パラメータ                          | 説明                                                  |
      | ---------------------------------- | ------------------------------------------------------------ |
      | dfs.nameservices                   | HDFSクラスターの名前。                                |
      | dfs.ha.namenodes.XXX               | HDFSクラスター内のNameNodeの名前。複数のNameNode名を指定する場合は、カンマ（`,`）で区切ります。`xxx`は`dfs.nameservices`で指定したHDFSクラスター名です。 |
      | dfs.namenode.rpc-address.XXX.NN    | HDFSクラスター内のNameNodeのRPCアドレス。`NN`は`dfs.ha.namenodes.XXX`で指定したNameNode名です。 |
      | dfs.client.failover.proxy.provider | クライアントが接続するNameNodeのプロバイダー。デフォルト値：`org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`。 |

  > **注意**
  >
  > StarRocksクラスターにデプロイされたブローカーを確認するには、[SHOW BROKER](../cluster-management/nodes_processes/SHOW_BROKER.md)文を使用できます。

#### AWS S3

AWS S3をストレージシステムとして選択した場合、次のいずれかのアクションを実行します：

- インスタンスプロファイルベースの認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- アサインされたロールベースの認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "aws.s3.use_instance_profile" = "true",
  "aws.s3.iam_role_arn" = "<iam_role_arn>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

- IAMユーザーベースの認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "aws.s3.use_instance_profile" = "false",
  "aws.s3.access_key" = "<iam_user_access_key>",
  "aws.s3.secret_key" = "<iam_user_secret_key>",
  "aws.s3.region" = "<aws_s3_region>"
  ```

`StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

| パラメータ                   | 必須 | 説明                                                  |
| --------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.use_instance_profile | はい      | 資格情報メソッドのインスタンスプロファイルとアサインされたロールを有効にするかどうかを指定します。有効な値：`true`および`false`。デフォルト値：`false`。 |
| aws.s3.iam_role_arn         | いいえ       | AWS S3バケットに対する権限を持つIAMロールのARN。AWS S3へのアクセスの資格情報メソッドとしてアサインされたロールを選択する場合、このパラメータを指定する必要があります。 |
| aws.s3.region               | はい      | AWS S3バケットが存在するリージョン。例：`us-west-1`。 |
| aws.s3.access_key           | いいえ       | IAMユーザーのアクセスキー。AWS S3へのアクセスの資格情報メソッドとしてIAMユーザーを選択する場合、このパラメータを指定する必要があります。 |
| aws.s3.secret_key           | いいえ       | IAMユーザーのシークレットキー。AWS S3へのアクセスの資格情報メソッドとしてIAMユーザーを選択する場合、このパラメータを指定する必要があります。 |

AWS S3へのアクセスのための認証方法の選択方法とAWS IAMコンソールでのアクセス制御ポリシーの設定方法については、[Authentication parameters for accessing AWS S3](../../../integrations/authenticate_to_aws_resources.md#authentication-parameters-for-accessing-aws-s3)を参照してください。

#### Google GCS

Google GCSをストレージシステムとして選択した場合、次のいずれかのアクションを実行します：

- VMベースの認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "gcp.gcs.use_compute_engine_service_account" = "true"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**                              | **デフォルト値** | **値の例** | **説明**                                              |
  | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
  | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engineにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |

- サービスアカウントベースの認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "gcp.gcs.service_account_email" = "<google_service_account_email>",
  "gcp.gcs.service_account_private_key_id" = "<google_service_private_key_id>",
  "gcp.gcs.service_account_private_key" = "<google_service_private_key>"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
  | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
  | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | サービスアカウントの作成時に生成されたJSONファイルのメールアドレス。 |
  | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | サービスアカウントの作成時に生成されたJSONファイルのプライベートキーID。 |
  | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | サービスアカウントの作成時に生成されたJSONファイルのプライベートキー。 |

- インパーソネーションベースの認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  - VMインスタンスにサービスアカウントをインパーソネートさせる：

    ```SQL
    "gcp.gcs.use_compute_engine_service_account" = "true",
    "gcp.gcs.impersonation_service_account" = "<assumed_google_service_account_email>"
    ```

    `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

    | **パラメータ**                              | **デフォルト値** | **値の例** | **説明**                                              |
    | ------------------------------------------ | ----------------- | --------------------- | ------------------------------------------------------------ |
    | gcp.gcs.use_compute_engine_service_account | false             | true                  | Compute Engineにバインドされたサービスアカウントを直接使用するかどうかを指定します。 |
    | gcp.gcs.impersonation_service_account      | ""                | "hello"               | インパーソネートしたいサービスアカウント。            |

  - サービスアカウント（メタサービスアカウントと呼ばれる）に別のサービスアカウント（データサービスアカウントと呼ばれる）をインパーソネートさせる：

    ```SQL
    "gcp.gcs.service_account_email" = "<google_service_account_email>",
    "gcp.gcs.service_account_private_key_id" = "<meta_google_service_account_email>",
    "gcp.gcs.service_account_private_key" = "<meta_google_service_account_email>",
    "gcp.gcs.impersonation_service_account" = "<data_google_service_account_email>"
    ```

    `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

    | **パラメータ**                          | **デフォルト値** | **値の例**                                        | **説明**                                              |
    | -------------------------------------- | ----------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
    | gcp.gcs.service_account_email          | ""                | `"user@hello.iam.gserviceaccount.com"` | メタサービスアカウントの作成時に生成されたJSONファイルのメールアドレス。 |
    | gcp.gcs.service_account_private_key_id | ""                | "61d257bd8479547cb3e04f0b9b6b9ca07af3b7ea"                   | メタサービスアカウントの作成時に生成されたJSONファイルのプライベートキーID。 |
    | gcp.gcs.service_account_private_key    | ""                | "-----BEGIN PRIVATE KEY----xxxx-----END PRIVATE KEY-----\n"  | メタサービスアカウントの作成時に生成されたJSONファイルのプライベートキー。 |
    | gcp.gcs.impersonation_service_account  | ""                | "hello"                                                      | インパーソネートしたいデータサービスアカウント。       |

#### その他のS3互換ストレージシステム

他のS3互換ストレージシステム（例：MinIO）を選択する場合、`StorageCredentialParams`を次のように設定します：

```SQL
"aws.s3.enable_ssl" = "false",
"aws.s3.enable_path_style_access" = "true",
"aws.s3.endpoint" = "<s3_endpoint>",
"aws.s3.access_key" = "<iam_user_access_key>",
"aws.s3.secret_key" = "<iam_user_secret_key>"
```

`StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

| パラメータ                        | 必須 | 説明                                                  |
| -------------------------------- | -------- | ------------------------------------------------------------ |
| aws.s3.enable_ssl                | はい      | SSL接続を有効にするかどうかを指定します。有効な値：`true`および`false`。デフォルト値：`true`。 |
| aws.s3.enable_path_style_access  | はい      | パススタイルのURLアクセスを有効にするかどうかを指定します。有効な値：`true`および`false`。デフォルト値：`false`。MinIOの場合、値を`true`に設定する必要があります。 |
| aws.s3.endpoint                  | はい      | AWS S3の代わりにS3互換ストレージシステムに接続するために使用されるエンドポイント。 |
| aws.s3.access_key                | はい      | IAMユーザーのアクセスキー。 |
| aws.s3.secret_key                | はい      | IAMユーザーのシークレットキー。 |

#### Microsoft Azure Storage

##### Azure Blob Storage

Blob Storageをストレージシステムとして選択する場合、次のいずれかのアクションを実行します：

- 共有キー認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.shared_key" = "<storage_account_shared_key>"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**              | **必須** | **説明**                              |
  | -------------------------- | ------------ | -------------------------------------------- |
  | azure.blob.storage_account | はい          | Blob Storageアカウントのユーザー名。   |
  | azure.blob.shared_key      | はい          | Blob Storageアカウントの共有キー。 |

- SASトークン認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.blob.storage_account" = "<storage_account_name>",
  "azure.blob.container" = "<container_name>",
  "azure.blob.sas_token" = "<storage_account_SAS_token>"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**             | **必須** | **説明**                                              |
  | ------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.blob.storage_account| はい          | Blob Storageアカウントのユーザー名。                   |
  | azure.blob.container      | はい          | データを保存するBlobコンテナの名前。        |
  | azure.blob.sas_token      | はい          | Blob Storageアカウントにアクセスするために使用されるSASトークン。 |

##### Azure Data Lake Storage Gen2

Data Lake Storage Gen2をストレージシステムとして選択する場合、次のいずれかのアクションを実行します：

- マネージドID認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.adls2.oauth2_use_managed_identity" = "true",
  "azure.adls2.oauth2_tenant_id" = "<service_principal_tenant_id>",
  "azure.adls2.oauth2_client_id" = "<service_client_id>"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**                           | **必須** | **説明**                                              |
  | --------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_use_managed_identity | はい          | マネージドID認証方法を有効にするかどうかを指定します。値を`true`に設定します。 |
  | azure.adls2.oauth2_tenant_id            | はい          | アクセスしたいテナントのID。          |
  | azure.adls2.oauth2_client_id            | はい          | マネージドIDのクライアント（アプリケーション）ID。         |

- 共有キー認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.adls2.storage_account" = "<storage_account_name>",
  "azure.adls2.shared_key" = "<storage_account_shared_key>"
  ```

`StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**               | **必須** | **説明**                                              |
  | --------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.storage_account | はい          | Data Lake Storage Gen2ストレージアカウントのユーザー名。 |
  | azure.adls2.shared_key      | はい          | Data Lake Storage Gen2ストレージアカウントの共有キー。 |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.adls2.oauth2_client_id" = "<service_client_id>",
  "azure.adls2.oauth2_client_secret" = "<service_principal_client_secret>",
  "azure.adls2.oauth2_client_endpoint" = "<service_principal_client_endpoint>"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**                      | **必須** | **説明**                                              |
  | ---------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls2.oauth2_client_id       | はい          | サービスプリンシパルのクライアント（アプリケーション）ID。        |
  | azure.adls2.oauth2_client_secret   | はい          | 作成された新しいクライアント（アプリケーション）シークレットの値。    |
  | azure.adls2.oauth2_client_endpoint | はい          | サービスプリンシパルまたはアプリケーションのOAuth 2.0トークンエンドポイント（v1）。 |

##### Azure Data Lake Storage Gen1

Data Lake Storage Gen1をストレージシステムとして選択する場合、次のいずれかのアクションを実行します：

- マネージドサービスID認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.adls1.use_managed_service_identity" = "true"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**                            | **必須** | **説明**                                              |
  | ---------------------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.use_managed_service_identity | はい          | マネージドサービスID認証方法を有効にするかどうかを指定します。値を`true`に設定します。 |

- サービスプリンシパル認証方法を選択するには、`StorageCredentialParams`を次のように設定します：

  ```SQL
  "azure.adls1.oauth2_client_id" = "<application_client_id>",
  "azure.adls1.oauth2_credential" = "<application_client_credential>",
  "azure.adls1.oauth2_endpoint" = "<OAuth_2.0_authorization_endpoint_v2>"
  ```

  `StorageCredentialParams`で設定する必要があるパラメータは次のように説明されています。

  | **パラメータ**                 | **必須** | **説明**                                              |
  | ----------------------------- | ------------ | ------------------------------------------------------------ |
  | azure.adls1.oauth2_client_id  | はい          | クライアント（アプリケーション）のID。                         |
  | azure.adls1.oauth2_credential | はい          | 作成された新しいクライアント（アプリケーション）シークレットの値。    |
  | azure.adls1.oauth2_endpoint   | はい          | サービスプリンシパルまたはアプリケーションのOAuth 2.0トークンエンドポイント（v1）。 |

### opt_properties

ロードジョブ全体に適用されるオプションのパラメータを指定します。構文：

```Plain
PROPERTIES ("<key1>" = "<value1>"[, "<key2>" = "<value2>" ...])
```

サポートされているパラメータは次のとおりです：

- `timeout`

  ロードジョブのタイムアウト期間を指定します。単位：秒。デフォルトのタイムアウト期間は4時間です。タイムアウト期間を6時間未満に指定することをお勧めします。ロードジョブがタイムアウト期間内に終了しない場合、StarRocksはロードジョブをキャンセルし、ロードジョブのステータスは**CANCELLED**になります。

  > **注意**
  >
  > ほとんどの場合、タイムアウト期間を設定する必要はありません。ロードジョブがデフォルトのタイムアウト期間内に終了しない場合にのみ、タイムアウト期間を設定することをお勧めします。

  タイムアウト期間を推測するには、次の式を使用します：

  **タイムアウト期間 > (ロードするデータファイルの総サイズ x ロードするデータファイルの総数およびデータファイルに作成されたマテリアライズドビューの数)/平均ロード速度**

  > **注意**
  >
  > 「平均ロード速度」は、StarRocksクラスター全体の平均ロード速度です。平均ロード速度は、サーバー構成やクラスターの最大同時クエリータスク数によって異なるため、クラスターごとに異なります。過去のロードジョブのロード速度に基づいて平均ロード速度を推測できます。

  たとえば、1GBのデータファイルをロードし、その上に2つのマテリアライズドビューを作成し、StarRocksクラスターの平均ロード速度が10 MB/sである場合、データロードに必要な時間は約102秒です。

  (1 x 1024 x 3)/10 = 307.2 (秒)

  この例では、タイムアウト期間を308秒以上に設定することをお勧めします。

- `max_filter_ratio`

  ロードジョブの最大エラー許容度を指定します。最大エラー許容度は、不十分なデータ品質の結果としてフィルタリングされる行の最大割合です。有効な値：`0`〜`1`。デフォルト値：`0`。

  - このパラメータを`0`に設定すると、StarRocksはロード中に不適格な行を無視しません。このため、ソースデータに不適格な行が含まれている場合、ロードジョブは失敗します。これにより、StarRocksにロードされるデータの正確性が保証されます。

  - このパラメータを`0`より大きい値に設定すると、StarRocksはロード中に不適格な行を無視できます。このため、ソースデータに不適格な行が含まれていても、ロードジョブは成功する可能性があります。

    > **注意**
    >
    > 不十分なデータ品質のためにフィルタリングされる行には、WHERE句によってフィルタリングされる行は含まれません。

  最大エラー許容度が`0`に設定されているためにロードジョブが失敗した場合、[SHOW LOAD](SHOW_LOAD.md)を使用してジョブ結果を表示できます。その後、不適格な行をフィルタリングできるかどうかを判断します。不適格な行をフィルタリングできる場合、ジョブ結果で返される`dpp.abnorm.ALL`と`dpp.norm.ALL`の値に基づいて最大エラー許容度を計算し、最大エラー許容度を調整してロードジョブを再送信します。最大エラー許容度を計算するための式は次のとおりです：

  `max_filter_ratio` = [`dpp.abnorm.ALL`/(`dpp.abnorm.ALL` + `dpp.norm.ALL`)]

  `dpp.abnorm.ALL`と`dpp.norm.ALL`の値の合計は、ロードされる行の総数です。

- `log_rejected_record_num`

  ログに記録できる不適格なデータ行の最大数を指定します。このパラメータはv3.1以降でサポートされています。有効な値：`0`、`-1`、および任意の非ゼロの正の整数。デフォルト値：`0`。

  - 値`0`は、フィルタリングされたデータ行がログに記録されないことを指定します。
  - 値`-1`は、フィルタリングされたすべてのデータ行がログに記録されることを指定します。
  - 非ゼロの正の整数（例：`n`）は、各BEで最大`n`のフィルタリングされたデータ行がログに記録されることを指定します。

- `load_mem_limit`

  ロードジョブに提供できる最大メモリ量を指定します。このパラメータの値は、各BEまたはCNノードでサポートされる上限メモリを超えることはできません。単位：バイト。デフォルトのメモリ制限は2 GBです。

- `strict_mode`

  [strict mode](../../../loading/load_concept/strict_mode.md)を有効にするかどうかを指定します。有効な値：`true`および`false`。デフォルト値：`false`。`true`はstrict modeを有効にし、`false`はstrict modeを無効にします。

- `timezone`

  ロードジョブのタイムゾーンを指定します。デフォルト値：`Asia/Shanghai`。タイムゾーンの設定は、strftime、alignment_timestamp、from_unixtimeなどの関数によって返される結果に影響を与えます。詳細については、[Configure a time zone](../../../administration/management/timezone.md)を参照してください。`timezone`パラメータで指定されたタイムゾーンはセッションレベルのタイムゾーンです。

- `priority`

  ロードジョブの優先度を指定します。有効な値：`LOWEST`、`LOW`、`NORMAL`、`HIGH`、および`HIGHEST`。デフォルト値：`NORMAL`。Broker LoadはFEパラメータ`max_broker_load_job_concurrency`を提供し、StarRocksクラスター内で同時に実行できるBroker Loadジョブの最大数を決定します。指定された時間内に送信されたBroker Loadジョブの数が最大数を超える場合、過剰なジョブはその優先度に基づいてスケジュールされるまで待機します。

  `QUEUEING`または`LOADING`状態にある既存のロードジョブの優先度を変更するには、[ALTER LOAD](ALTER_LOAD.md)文を使用できます。

  StarRocksはv2.5以降、Broker Loadジョブの`priority`パラメータの設定を許可しています。

- `merge_condition`

  更新が有効になるかどうかを判断するために使用したい列の名前を指定します。ソースレコードから宛先レコードへの更新は、指定された列でソースデータレコードが宛先データレコードよりも大きいか等しい値を持つ場合にのみ有効になります。

  > **注意**
  >
  > 指定する列は主キー列であってはなりません。さらに、主キーテーブルを使用するテーブルのみが条件付き更新をサポートしています。

## 列マッピング

データファイルの列がStarRocksテーブルの列に順番に1対1でマッピングできる場合、データファイルとStarRocksテーブル間の列マッピングを設定する必要はありません。

データファイルの列がStarRocksテーブルの列に順番に1対1でマッピングできない場合、`columns`パラメータを使用してデータファイルとStarRocksテーブル間の列マッピングを設定する必要があります。これには次の2つのユースケースが含まれます：

- **同じ数の列だが異なる列順序。** **また、データファイルからのデータは、StarRocksテーブルの対応する列にロードされる前に関数によって計算される必要はありません。**

  `columns`パラメータでは、データファイルの列が配置されている順序と同じ順序でStarRocksテーブルの列名を指定する必要があります。

  たとえば、StarRocksテーブルは順番に`col1`、`col2`、`col3`の3つの列で構成されており、データファイルも3つの列で構成されており、順番にStarRocksテーブルの`col3`、`col2`、`col1`にマッピングできます。この場合、`"columns: col3, col2, col1"`を指定する必要があります。

- **異なる数の列と異なる列順序。また、データファイルからのデータは、StarRocksテーブルの対応する列にロードされる前に関数によって計算される必要があります。**

  `columns`パラメータでは、データファイルの列が配置されている順序と同じ順序でStarRocksテーブルの列名を指定し、データを計算するために使用したい関数を指定する必要があります。2つの例は次のとおりです：

  - StarRocksテーブルは順番に`col1`、`col2`、`col3`の3つの列で構成されています。データファイルは4つの列で構成されており、そのうち最初の3つの列は順番にStarRocksテーブルの`col1`、`col2`、`col3`にマッピングされ、4番目の列はStarRocksテーブルのいずれの列にもマッピングされません。この場合、データファイルの4番目の列に一時的な名前を指定する必要があり、その一時的な名前はStarRocksテーブルの列名とは異なる必要があります。たとえば、`"columns: col1, col2, col3, temp"`と指定できます。この場合、データファイルの4番目の列は一時的に`temp`と名付けられます。
  - StarRocksテーブルは順番に`year`、`month`、`day`の3つの列で構成されています。データファイルは、`yyyy-mm-dd hh:mm:ss`形式の日付と時刻の値を含む1つの列のみで構成されています。この場合、`"columns: col, year = year(col), month=month(col), day=day(col)"`と指定できます。この場合、`col`はデータファイル列の一時的な名前であり、関数`year = year(col)`、`month=month(col)`、および`day=day(col)`はデータファイル列`col`からデータを抽出し、対応するStarRocksテーブルの列にデータをロードするために使用されます。たとえば、`year = year(col)`はデータファイル列`col`から`yyyy`データを抽出し、StarRocksテーブル列`year`にデータをロードするために使用されます。

詳細な例については、[Configure column mapping](#configure-column-mapping)を参照してください。

## 関連する設定項目

FE設定項目`max_broker_load_job_concurrency`は、StarRocksクラスター内で同時に実行できるBroker Loadジョブの最大数を指定します。

StarRocks v2.4以前では、特定の期間内に送信されたBroker Loadジョブの総数が最大数を超える場合、過剰なジョブはその送信時間に基づいてキューに入れられ、スケジュールされます。

StarRocks v2.5以降では、特定の期間内に送信されたBroker Loadジョブの総数が最大数を超える場合、過剰なジョブはその優先度に基づいてキューに入れられ、スケジュールされます。上記で説明した`priority`パラメータを使用してジョブの優先度を指定できます。`QUEUEING`または`LOADING`状態にある既存のジョブの優先度を変更するには、[ALTER LOAD](ALTER_LOAD.md)を使用できます。

## ジョブの分割と同時実行

Broker Loadジョブは、1つ以上のタスクに分割され、同時に実行されることができます。ロードジョブ内のタスクは単一のトランザクション内で実行されます。それらはすべて成功するか失敗する必要があります。StarRocksは、`LOAD`文で`data_desc`をどのように宣言するかに基づいて各ロードジョブを分割します：

- 複数の`data_desc`パラメータを宣言し、それぞれが異なるテーブルを指定している場合、各テーブルのデータをロードするためのタスクが生成されます。

- 複数の`data_desc`パラメータを宣言し、それぞれが同じテーブルの異なるパーティションを指定している場合、各パーティションのデータをロードするためのタスクが生成されます。

さらに、各タスクは1つ以上のインスタンスに分割され、StarRocksクラスターのBEsまたはCNsに均等に分配され、同時に実行されます。StarRocksは、FEパラメータ`min_bytes_per_broker_scanner`とBEまたはCNノードの数に基づいて各タスクを分割します。個々のタスク内のインスタンス数を計算するには、次の式を使用します：

**個々のタスク内のインスタンス数 = min(個々のタスクによってロードされるデータ量/`min_bytes_per_broker_scanner`, BE/CNノードの数)**

ほとんどの場合、各ロードジョブには1つの`data_desc`のみが宣言されており、各ロードジョブは1つのタスクにのみ分割され、そのタスクはBEまたはCNノードの数と同じ数のインスタンスに分割されます。

## 例

このセクションでは、HDFSを例にとってさまざまなロード設定を説明します。

### CSVデータのロード

このセクションでは、CSVを例にとって、さまざまなロード要件を満たすために使用できるさまざまなパラメータ設定を説明します。

#### タイムアウト期間の設定

StarRocksデータベース`test_db`には、`table1`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example1.csv`も3つの列で構成されており、順番に`table1`の`col1`、`col2`、`col3`にマッピングされています。

`example1.csv`から`table1`に3600秒以内にすべてのデータをロードしたい場合、次のコマンドを実行します：

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

StarRocksデータベース`test_db`には、`table2`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example2.csv`も3つの列で構成されており、順番に`table2`の`col1`、`col2`、`col3`にマッピングされています。

`example2.csv`から`table2`に最大エラー許容度`0.1`でデータをロードしたい場合、次のコマンドを実行します：

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

StarRocksデータベース`test_db`には、`table3`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

HDFSクラスターの`/user/starrocks/data/input/`パスに保存されているすべてのデータファイルも、それぞれ3つの列で構成されており、順番に`table3`の`col1`、`col2`、`col3`にマッピングされています。これらのデータファイルで使用される列区切り文字は`\x01`です。

HDFSサーバー上の`hdfs://<hdfs_host>:<hdfs_port>/user/starrocks/data/input/`パスに保存されているすべてのデータファイルから`table3`にデータをロードしたい場合、次のコマンドを実行します：

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

#### NameNode HAメカニズムの設定

StarRocksデータベース`test_db`には、`table4`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example4.csv`も3つの列で構成されており、`table4`の`col1`、`col2`、`col3`にマッピングされています。

NameNodeのHAメカニズムが設定された状態で`example4.csv`から`table4`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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

#### Kerberos認証の設定

StarRocksデータベース`test_db`には、`table5`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example5.csv`も3つの列で構成されており、順番に`table5`の`col1`、`col2`、`col3`にマッピングされています。

Kerberos認証が設定され、keytabファイルパスが指定された状態で`example5.csv`から`table5`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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

StarRocksデータベース`test_db`には、`table6`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example6.csv`も3つの列で構成されており、順番に`table6`の`col1`、`col2`、`col3`にマッピングされています。

Broker Loadジョブを実行して`example6.csv`から`table6`にすべてのデータをロードしました。

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

StarRocksデータベース`test_db`には、`table7`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example7.csv`も3つの列で構成されており、順番に`table7`の`col1`、`col2`、`col3`にマッピングされています。

`example7.csv`から`table7`の2つのパーティション`p1`と`p2`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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

StarRocksデータベース`test_db`には、`table8`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example8.csv`も3つの列で構成されており、順番に`table8`の`col2`、`col1`、`col3`にマッピングされています。

`example8.csv`から`table8`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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
> 前述の例では、`example8.csv`の列は`table8`の列と同じ順序でマッピングできません。そのため、`column_list`を使用して`example8.csv`と`table8`の間の列マッピングを設定する必要があります。

#### フィルタ条件の設定

StarRocksデータベース`test_db`には、`table9`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example9.csv`も3つの列で構成されており、順番に`table9`の`col1`、`col2`、`col3`にマッピングされています。

`example9.csv`から`table9`に、最初の列の値が`20180601`より大きいデータレコードのみをロードしたい場合、次のコマンドを実行します：

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
> 前述の例では、`example9.csv`の列は`table9`の列と同じ順序でマッピングできますが、WHERE句を使用して列ベースのフィルタ条件を指定する必要があります。そのため、`column_list`を使用して`example9.csv`と`table9`の間の列マッピングを設定する必要があります。

#### HLL型の列を含むテーブルへのデータロード

StarRocksデータベース`test_db`には、`table10`という名前のテーブルがあります。このテーブルは、順番に`id`、`col1`、`col2`、`col3`の4つの列で構成されています。`col1`と`col2`はHLL型の列として定義されています。

データファイル`example10.csv`は3つの列で構成されており、最初の列は`table10`の`id`にマッピングされ、2番目と3番目の列は順番に`table10`の`col1`と`col2`にマッピングされています。`example10.csv`の2番目と3番目の列の値は、関数を使用してHLL型データに変換され、`table10`の`col1`と`col2`にロードされます。

`example10.csv`から`table10`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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
> 前述の例では、`example10.csv`の3つの列は`column_list`を使用して順番に`id`、`temp1`、`temp2`と名付けられます。その後、関数を使用してデータを変換します：
>
> - `hll_hash`関数は、`example10.csv`の`temp1`と`temp2`の値をHLL型データに変換し、`example10.csv`の`temp1`と`temp2`を`table10`の`col1`と`col2`にマッピングします。
>
> - `hll_empty`関数は、指定されたデフォルト値を`table10`の`col3`に埋め込むために使用されます。

関数`hll_hash`および`hll_empty`の使用については、[hll_hash](../../sql-functions/scalar-functions/hll_hash.md)および[hll_empty](../../sql-functions/scalar-functions/hll_empty.md)を参照してください。

#### ファイルパスからパーティションフィールドの値を抽出

Broker Loadは、宛先StarRocksテーブルの列定義に基づいて、ファイルパスに含まれる特定のパーティションフィールドの値を解析することをサポートしています。このStarRocksの機能は、Apache Spark™のPartition Discovery機能に似ています。

StarRocksデータベース`test_db`には、`table11`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`、`city`、`utc_date`の5つの列で構成されています。

HDFSクラスターのファイルパス`/user/starrocks/data/input/dir/city=beijing`には、次のデータファイルが含まれています：

- `/user/starrocks/data/input/dir/city=beijing/utc_date=2019-06-26/0000.csv`

- `/user/starrocks/data/input/dir/city=beijing/utc_date=2019-06-26/0001.csv`

これらのデータファイルはそれぞれ3つの列で構成されており、順番に`table11`の`col1`、`col2`、`col3`にマッピングされています。

ファイルパス`/user/starrocks/data/input/dir/city=beijing/utc_date=*/*`からすべてのデータファイルを`table11`にロードし、同時にファイルパスに含まれるパーティションフィールド`city`と`utc_date`の値を抽出して`table11`の`city`と`utc_date`にロードしたい場合、次のコマンドを実行します：

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

#### `%3A`を含むファイルパスからパーティションフィールドの値を抽出

HDFSでは、ファイルパスにコロン（:）を含めることはできません。すべてのコロン（:）は`%3A`に変換されます。

StarRocksデータベース`test_db`には、`table12`という名前のテーブルがあります。このテーブルは、順番に`data_time`、`col1`、`col2`の3つの列で構成されています。テーブルスキーマは次のとおりです：

```SQL
data_time DATETIME,
col1        INT,
col2        INT
```

HDFSクラスターのファイルパス`/user/starrocks/data`には、次のデータファイルが含まれています：

- `/user/starrocks/data/data_time=2020-02-17 00%3A00%3A00/example12.csv`

- `/user/starrocks/data/data_time=2020-02-18 00%3A00%3A00/example12.csv`

`example12.csv`から`table12`にすべてのデータをロードし、同時にファイルパスからパーティションフィールド`data_time`の値を抽出して`table12`の`data_time`にロードしたい場合、次のコマンドを実行します：

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
> 前述の例では、パーティションフィールド`data_time`から抽出された値は、`2020-02-17 00%3A00%3A00`のように`%3A`を含む文字列です。したがって、`str_to_date`関数を使用して、文字列を`data_time`にロードする前にDATETIME型データに変換する必要があります。

#### `format_type_options`の設定

StarRocksデータベース`test_db`には、`table13`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example13.csv`も3つの列で構成されており、順番に`table13`の`col2`、`col1`、`col3`にマッピングされています。

`example13.csv`から`table13`にすべてのデータをロードし、`example13.csv`の最初の2行をスキップし、列区切り文字の前後のスペースを削除し、`enclose`を`\`に設定し、`escape`を`\`に設定したい場合、次のコマンドを実行します：

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

### Parquetデータのロード

このセクションでは、Parquetデータをロードする際に注意すべきパラメータ設定について説明します。

StarRocksデータベース`test_db`には、`table13`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example13.parquet`も3つの列で構成されており、順番に`table13`の`col1`、`col2`、`col3`にマッピングされています。

`example13.parquet`から`table13`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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
> デフォルトでは、Parquetデータをロードする際、StarRocksはファイル名に**.parquet**拡張子が含まれているかどうかに基づいてデータファイル形式を決定します。ファイル名に**.parquet**拡張子が含まれていない場合、`FORMAT AS`を使用してデータファイル形式を`Parquet`として指定する必要があります。

### ORCデータのロード

このセクションでは、ORCデータをロードする際に注意すべきパラメータ設定について説明します。

StarRocksデータベース`test_db`には、`table14`という名前のテーブルがあります。このテーブルは、順番に`col1`、`col2`、`col3`の3つの列で構成されています。

データファイル`example14.orc`も3つの列で構成されており、順番に`table14`の`col1`、`col2`、`col3`にマッピングされています。

`example14.orc`から`table14`にすべてのデータをロードしたい場合、次のコマンドを実行します：

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
> - デフォルトでは、ORCデータをロードする際、StarRocksはファイル名に**.orc**拡張子が含まれているかどうかに基づいてデータファイル形式を決定します。ファイル名に**.orc**拡張子が含まれていない場合、`FORMAT AS`を使用してデータファイル形式を`ORC`として指定する必要があります。
>
> - StarRocks v2.3以前では、データファイルにARRAY型の列が含まれている場合、ORCデータファイルの列名がStarRocksテーブルの対応する列名と同じであることを確認する必要があり、SET句で列を指定することはできません。