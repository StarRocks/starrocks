---
displayed_sidebar: docs
description: "v3.4.0 以降、StarRocks は Python UDF の作成をサポートしています。"
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
- Python UDFには以下のパッケージが必要です： pyarrow
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

## Python UDF 実行のサンドボックス化

デフォルトでは、Python UDF ワーカーは**ランタイム分離なし**で動作します。これは BE が起動する `python3` プロセスで、BE と同じ OS ユーザーとして、完全なファイルシステムおよびネットワークアクセス権で UDF コードを実行します。実効的なセキュリティ境界は「誰が関数を作成/実行できるか」であり、`CREATE FUNCTION` / `CREATE GLOBAL FUNCTION` 権限は「BE の OS ユーザーとして任意コードを実行する権限」と同等とみなして付与してください。

UDF コードができることを制限するには、BE 設定項目 `python_udf_sandbox` でサンドボックスを有効にします。

| 値 | 分離 | 前提条件 |
| --- | --- | --- |
| `off`(デフォルト) | なし | — |
| `seccomp` | namespace を使わず、nsjail 経由で seccomp システムコールフィルター(`ptrace`、`mount`、カーネルモジュールのロード等を遮断)を適用 | 追加権限不要、任意のコンテナで動作 |
| `nsjail` | 完全な Linux namespace 分離(mount / network / PID / user namespace):最小限の読み取り専用ファイルシステムビュー、外部ネットワークなし、非特権 UID へのマッピング | ホストが選択した namespace モードを許可している必要あり(下記参照) |

サンドボックスは `be.conf` で設定する**デプロイ側**の制御項目で、UDF の作成者やクエリからは変更できません。分離ポリシー自体は運用者が編集可能な設定ファイル(`conf/pyudf-nsjail.conf`、`conf/pyudf-nsjail-seccomp.conf`、`conf/pyudf.kafel`)にあり、BE は起動時にデプロイ用のパスをそこにレンダリングするだけです。

### 例:サンドボックスを有効にする

1. 各 BE の `be.conf` に以下を追加します(`pyarrow` がインストールされた Python 環境が必要。[前提条件](#前提条件)を参照):

   ```Properties
   # pyarrow がインストールされた Python 環境(その bin/python3 が使われる)
   python_envs = /usr

   # サンドボックスレベル:off | seccomp | nsjail
   python_udf_sandbox = nsjail
   # nsjail の namespace 戦略:auto | rootless | privileged
   python_udf_sandbox_mode = auto
   # true の場合、要求したサンドボックスを確立できないと UDF の実行を拒否
   python_udf_sandbox_required = false
   ```

2. BE を再起動し、通常どおり UDF を作成・実行します:

   ```SQL
   CREATE FUNCTION py_add(INT, INT)
   RETURNS INT
   type = 'Python'
   symbol = 'add'
   file = 'inline'
   AS
   $$
   def add(a, b):
       return a + b
   $$
   ;

   SELECT py_add(1, 2);   -- 3 を返す(サンドボックス内で計算)
   ```

BE はワーカーに選択した実効サンドボックスレベルをログに記録します。BE ログで `python UDF nsjail sandbox` を検索してください。

### namespace モードとデプロイ前提条件

`python_udf_sandbox_mode` は `python_udf_sandbox = nsjail` の場合のみ有効です。

- `auto`(デフォルト):ホストが許可する場合は非特権ユーザー namespace を使用し、そうでなければ BE が `CAP_SYS_ADMIN` を持つ場合はそれを使用し、いずれも不可なら(`python_udf_sandbox_required = true` でない限り)`seccomp` のみに降格します。
- `rootless`:非特権ユーザー namespace を強制。ホストが非特権ユーザー namespace を許可し、**かつ** BE コンテナの seccomp/AppArmor が `unshare` と `mount` システムコールを許可している必要があります。
- `privileged`:ホストの `CAP_SYS_ADMIN` を強制(ユーザー namespace なし)。

Kubernetes では、コンテナのデフォルト seccomp / AppArmor プロファイルが namespace / mount システムコールを遮断するため、`nsjail` モードでは BE Pod でそれらを緩和する必要があります。rootless パスの場合、非特権ユーザー namespace を許可するノード上で、BE コンテナを unconfined な seccomp/AppArmor で実行します:

```yaml
securityContext:
  seccompProfile:
    type: Unconfined
  # さらに当該コンテナの AppArmor 'unconfined'、およびノード上の
  # kernel.unprivileged_userns_clone=1(AppArmor 制限なし)が必要
```

ユーザー namespace を許可できない環境では、`python_udf_sandbox = seccomp`(追加権限不要)を使用してください。これはシステムコールをフィルタしますが、ファイルシステムやネットワークは**分離しません**。それらはコンテナ自身の手段(例:Kubernetes `NetworkPolicy`)に依存してください。

:::note
`nsjail`/`seccomp` レベルには `python_udf_nsjail_path` が指す `nsjail` バイナリ(デフォルト `${STARROCKS_HOME}/lib/nsjail`)が必要です。存在せず `python_udf_sandbox_required = false` の場合、ワーカーはサンドボックスなしで実行され、警告がログに記録されます。
:::

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
| ARRAY                                | list                    |
| MAP                                  | dict                    |
| STRUCT                               | dict（フィールド名をキーとする） |
| JSON                                 | str（`json.loads` でパース） |
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
| MAP                                  | pyarrow.MapArray        |
| STRUCT                               | pyarrow.StructArray     |

ネスト型（`ARRAY`、`MAP`、`STRUCT`）は任意に組み合わせ可能です。例えば
`array<map<string,int>>`、`map<string,array<int>>`、
`struct<a array<int>, b map<string,int>>`、`array<struct<...>>` などです。
`scalar` モードでは、各行は再帰的に Python の `list` / `dict` に変換されて渡
されます。ネスト型を返すには、対応する Python の `list` / `dict` を返してくだ
さい（`dict` のキーは struct のフィールド名）。`pyarrow` が再帰的な変換を自動
で行います。

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
