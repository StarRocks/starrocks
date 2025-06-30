---
displayed_sidebar: docs
sidebar_position: 0.91
---

import Experimental from '../../_assets/commonMarkdown/_experimental.mdx'

# Python UDF

<Experimental />

このトピックでは、Python を使用してユーザー定義関数 (UDF) を開発する方法について説明します。

v3.4.0 以降、StarRocks は Python UDF の作成をサポートしています。

現在、StarRocks は Python でのスカラー UDF のみをサポートしています。

## 前提条件

次の要件を満たしていることを確認してください。

- [Python 3.8](https://www.python.org/downloads/release/python-380/) 以降をインストールします。
- StarRocks で UDF を有効にするには、FE 設定ファイル **fe/conf/fe.conf** で `enable_udf` を `true` に設定し、FE ノードを再起動して設定を有効にします。詳細については、[FE configuration - enable_udf](../../administration/management/FE_configuration.md#enable_udf) を参照してください。
- 環境変数を使用して BE インスタンスで Python インタープリタ環境の場所を設定します。変数項目 `python_envs` を追加し、Python インタープリタのインストール場所に設定します。例: `/opt/Python-3.8/`。

## Python UDF の開発と使用

構文:

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
{PROPERTIES ("key" = "value" [, ...]) | key="value" [...] }
[AS $$ $$]
```

| **パラメータ**    | **必須** | **説明**                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | No       | グローバル UDF を作成するかどうか。                              |
| function_name | Yes      | 作成したい関数の名前。このパラメータにはデータベース名を含めることができます。例: `db1.my_func`。`function_name` にデータベース名が含まれている場合、UDF はそのデータベースに作成されます。それ以外の場合、UDF は現在のデータベースに作成されます。新しい関数の名前とそのパラメータは、宛先データベースに既存の名前と同じにすることはできません。それ以外の場合、関数は作成できません。関数名が同じでもパラメータが異なる場合、作成は成功します。 |
| arg_type      | Yes      | 関数の引数の型。追加された引数は `, ...` で表すことができます。サポートされているデータ型については、[SQL データ型と Python データ型のマッピング](#sql-データ型と-python-データ型のマッピング) を参照してください。 |
| return_type   | Yes      | 関数の戻り値の型。サポートされているデータ型については、[SQL データ型と Python データ型のマッピング](#sql-データ型と-python-データ型のマッピング) を参照してください。 |
| PROPERTIES    | Yes      | 作成する UDF の種類に応じて異なる関数のプロパティ。 |
| AS $$ $$      | No       | `$$` マークの間にインライン UDF コードを指定します。              |

プロパティには次のものが含まれます。

| **プロパティ**  | **必須** | **説明**                                                  |
| ------------- | ------------ | ---------------------------------------------------------------- |
| type          | Yes          | UDF の種類。`Python` に設定すると、Python ベースの UDF を作成することを示します。 |
| symbol        | Yes          | UDF が属する Python プロジェクトのクラス名。このパラメータの値は `<package_name>.<class_name>` 形式です。 |
| input         | No           | 入力の種類。有効な値: `scalar` (デフォルト) および `arrow` (ベクトル化された入力)。     |
| file          | No           | UDF のコードを含む Python パッケージファイルをダウンロードできる HTTP URL。このパラメータの値は `http://<http_server_ip>:<http_server_port>/<python_package_name>` 形式です。パッケージ名には `.py.zip` サフィックスが必要です。デフォルト値: `inline`、インライン UDF を作成することを示します。 |

### Python を使用してインラインスカラー入力 UDF を作成する

次の例では、Python を使用してスカラー入力を持つインライン `echo` 関数を作成します。

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

次の例では、Python を使用してベクトル化入力を持つインライン `add` 関数を作成します。

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

Python パッケージを作成する際には、モジュールを `.py.zip` ファイルにパッケージ化する必要があります。これらは [zipimport format](https://docs.python.org/3/library/zipimport.html) を満たす必要があります。

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

## 付録

### SQL データ型と Python データ型のマッピング

| SQL タイプ                             | Python 3 タイプ           |
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

### Python のコンパイル

Python をコンパイルするには、次の手順に従います。

1. OpenSSL パッケージを取得します。

   ```Bash
   wget 'https://github.com/openssl/openssl/archive/OpenSSL_1_1_1m.tar.gz'
   ```

2. パッケージを解凍します。

   ```Bash
   tar -zxf OpenSSL_1_1_1m.tar.gz
   ```

3. 解凍されたフォルダに移動します。

   ```Bash
   cd openssl-OpenSSL_1_1_1m
   ```

4. 環境変数 `OPENSSL_DIR` を設定します。

   ```Bash
   export OPENSSL_DIR=`pwd`/install
   ```

5. コンパイルのためのソースコードを準備します。

   ```Bash
   ./Configure --prefix=`pwd`/install
   ./config --prefix=`pwd`/install
   ```

6. OpenSSL をコンパイルします。

   ```Bash
   make -j 16 && make install
   ```

7. 環境変数 `LD_LIBRARY_PATH` を設定します。

   ```Bash
   LD_LIBRARY_PATH=$OPENSSL_DIR/lib:$LD_LIBRARY_PATH
   ```

8. 作業ディレクトリに戻り、Python パッケージを取得します。

   ```Bash
   wget 'https://www.python.org/ftp/python/3.12.9/Python-3.12.9.tgz'
   ```
9. パッケージを解凍します。

   ```Bash
   tar -zxf ./Python-3.12.9.tgz 
   ```

10. 解凍されたフォルダに移動します。

   ```Bash
   cd Python-3.12.9
   ```

11. ディレクトリ `build` を作成し、そこに移動します。

   ```Bash
   mkdir build && cd build
   ```

12. コンパイルのためのソースコードを準備します。

   ```Bash
   ../configure --prefix=`pwd`/install --with-openssl=$OPENSSL_DIR
   ```

13. Python をコンパイルします。

   ```Bash
   make -j 16 && make install
   ```

14. PyArrow と grpcio をインストールします。

   ```Bash
   ./install/bin/pip3 install pyarrow grpcio
   ```

15. ファイルをパッケージに圧縮します。

   ```Bash
   tar -zcf ./Python-3.12.9.tar.gz install
   ```

16. パッケージをターゲット BE サーバーに配布し、パッケージを解凍します。

   ```Bash
   tar -zxf ./Python-3.12.9.tar.gz
   ```

17. BE 構成ファイル **be.conf** を修正し、次の構成項目を追加します。

   ```Properties
   python_envs=/home/disk1/sr/install
   ```
