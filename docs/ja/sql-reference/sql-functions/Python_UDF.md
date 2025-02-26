---
displayed_sidebar: docs
sidebar_position: 0.91
---

# Python UDF

このトピックでは、Python を使用してユーザー定義関数 (UDF) を開発する方法について説明します。

v3.4.0 以降、StarRocks は Python UDF の作成をサポートしています。

現在、StarRocks は Python でのスカラー UDF のみをサポートしています。

## 前提条件

次の要件を満たしていることを確認してください:

- [Python 3.8](https://www.python.org/downloads/release/python-380/) 以降をインストールします。
- StarRocks で UDF を有効にするには、FE 設定ファイル **fe/conf/fe.conf** で `enable_udf` を `true` に設定し、設定が有効になるように FE ノードを再起動します。詳細については、[FE configuration - enable_udf](../../administration/management/FE_configuration.md#enable_udf) を参照してください。
- 環境変数を使用して、BE インスタンスで Python インタープリタ環境の場所を設定します。変数項目 `python_envs` を追加し、Python インタープリタのインストール場所を設定します。例: `/opt/Python-3.8/`。

## Python UDF の開発と使用

構文:

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
{PROPERTIES ("key" = "value" [, ...]) | key="value" [...] }
[AS $$ $$]
```

| **パラメータ**   | **必須** | **説明** |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | No       | グローバル UDF を作成するかどうか。 |
| function_name | Yes      | 作成したい関数の名前。このパラメータにはデータベース名を含めることができます。例: `db1.my_func`。`function_name` にデータベース名が含まれている場合、UDF はそのデータベースに作成されます。そうでない場合、UDF は現在のデータベースに作成されます。新しい関数の名前とそのパラメータは、宛先データベース内の既存の名前と同じにすることはできません。ただし、関数名が同じでもパラメータが異なる場合は作成に成功します。 |
| arg_type      | Yes      | 関数の引数の型。追加された引数は `, ...` で表すことができます。サポートされているデータ型については、[SQL データ型と Python データ型のマッピング](#mapping-between-sql-data-types-and-python-data-types) を参照してください。 |
| return_type   | Yes      | 関数の戻り値の型。サポートされているデータ型については、[SQL データ型と Python データ型のマッピング](#mapping-between-sql-data-types-and-python-data-types) を参照してください。 |
| PROPERTIES    | Yes      | 関数のプロパティ。作成する UDF のタイプによって異なります。 |
| AS $$ $$      | No       | `$$` マークの間にインライン UDF コードを指定します。 |

プロパティには以下が含まれます:

| **プロパティ** | **必須** | **説明** |
| ------------- | ------------ | ---------------------------------------------------------------- |
| type          | Yes          | UDF のタイプ。`Python` に設定すると、Python ベースの UDF を作成することを示します。 |
| symbol        | Yes          | UDF が属する Python プロジェクトのクラス名。このパラメータの値は `<package_name>.<class_name>` 形式です。 |
| input         | No           | 入力のタイプ。有効な値: `scalar` (デフォルト) および `arrow` (ベクトル化された入力)。 |
| file          | No           | UDF のコードを含む Python パッケージファイルをダウンロードできる HTTP URL。このパラメータの値は `http://<http_server_ip>:<http_server_port>/<python_package_name>` 形式です。パッケージ名には `.py.zip` のサフィックスが必要です。デフォルト値: `inline`、これはインライン UDF を作成することを示します。 |

### Python を使用してインラインスカラー入力 UDF を作成する

次の例は、Python を使用してスカラー入力のインライン `echo` 関数を作成します。

```SQL
CREATE FUNCTION python_echo(INT)
RETURNS INT
type = 'Python'
symbol = 'echo'
file = 'inline'
AS
$$
def echo(x):
    return x
$$
;
```

### Python を使用してインラインベクトル化入力 UDF を作成する

ベクトル化入力は、UDF 処理のパフォーマンスを向上させるためにサポートされています。

次の例は、Python を使用してベクトル化入力のインライン `add` 関数を作成します。

```SQL
CREATE FUNCTION python_add(INT) 
RETURNS INT
type = 'Python'
symbol = 'add'
input = "arrow"
AS
$$
import pyarrow.compute as pc
def add(x):
    return pc.add(x, 1)
$$
;
```

### Python を使用してパッケージ化された UDF を作成する

Python パッケージを作成する際、モジュールを `.py.zip` ファイルにパッケージ化する必要があります。これらは [zipimport format](https://docs.python.org/3/library/zipimport.html) に準拠している必要があります。

```Plain
> tree .
.
├── main.py
└── yaml
    ├── composer.py
    ├── constructor.py
    ├── cyaml.py
    ├── dumper.py
    ├── emitter.py
    ├── error.py
    ├── events.py
    ├── __init__.py
    ├── loader.py
    ├── nodes.py
    ├── parser.py
```

```Plain
> cat main.py 
import numpy
import yaml

def echo(a):
    return yaml.__version__
```

```SQL
CREATE FUNCTION py_pack(string) 
RETURNS  string 
symbol = "add"
type = "Python"
file = "http://HTTP_IP:HTTP_PORT/m1.py.zip"
symbol = "main.echo"
;
```

## SQL データ型と Python データ型のマッピング

| SQL Type                             | Python 3 Type           |
| ------------------------------------ | ----------------------- |
| **SCALAR**                           |                         |
| TINYINT/SMALLINT/INT/BIGINT/LARGEINT | INT                     |
| STRING                               | string                  |
| DOUBLE                               | FLOAT                   |
| BOOLEAN                              | BOOL                    |
| DATETIME                             | DATETIME.DATETIME       |
| FLOAT                                | FLOAT                   |
| CHAR                                 | STRING                  |
| VARCHAR                              | STRING                  |
| DATE                                 | DATETIME.DATE           |
| DECIMAL                              | DECIMAL.DECIMAL         |
| ARRAY                                | List                    |
| MAP                                  | Dict                    |
| STRUCT                               | COLLECTIONS.NAMEDTUPLE  |
| JSON                                 | dict                    |
| **VECTORIZED**                       |                         |
| TYPE_BOOLEAN                         | pyarrow.lib.BoolArray   |
| TYPE_TINYINT                         | pyarrow.lib.Int8Array   |
| TYPE_SMALLINT                        | pyarrow.lib.Int15Array  |
| TYPE_INT                             | pyarrow.lib.Int32Array  |
| TYPE_BIGINT                          | pyarrow.lib.Int64Array  |
| TYPE_FLOAT                           | pyarrow.FloatArray      |
| TYPE_DOUBLE                          | pyarrow.DoubleArray     |
| VARCHAR                              | pyarrow.StringArray     |
| DECIMAL                              | pyarrow.Decimal128Array |
| DATE                                 | pyarrow.Date32Array     |
| TYPE_TIME                            | pyarrow.TimeArray       |
| ARRAY                                | pyarrow.ListArray       |