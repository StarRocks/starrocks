---
description: StarRocks と Jupyter ノートブック
displayed_sidebar: docs
---

# Jupyter

このガイドでは、ノートブック、コード、およびデータのための最新のウェブベースのインタラクティブ開発環境である [Jupyter](https://jupyter.org/) と StarRocks クラスターを統合する方法を説明します。

これらはすべて、[JupySQL](https://jupysql.ploomber.io/) を介して可能になります。JupySQL は、%sql、%%sql、および %sqlplot マジックを使用して Jupyter で SQL を実行し、大規模なデータセットをプロットすることを可能にします。

JupySQL を Jupyter 上で使用して、StarRocks 上でクエリを実行することができます。

データがクラスターにロードされると、SQL プロットを介してデータをクエリし、視覚化することができます。

## 前提条件

始める前に、以下のソフトウェアがローカルにインストールされている必要があります：

- [JupySQL](https://jupysql.ploomber.io/en/latest/quick-start.html): `pip install jupysql`
- Jupyterlab: `pip install jupyterlab`
- [SKlearn Evaluation](https://github.com/ploomber/sklearn-evaluation): `pip install sklearn-evaluation`
- Python
- pymysql: `pip install pymysql`

> **NOTE**
>
> 上記の要件を満たしたら、`jupyterlab` を呼び出すだけで Jupyter lab を開くことができます。これにより、ノートブックインターフェースが開きます。
> すでにノートブックで Jupyter lab が実行されている場合は、以下のセルを実行して依存関係を取得することができます。

```python
# 必要なパッケージをインストールします。
%pip install --quiet jupysql sklearn-evaluation pymysql
```

> **NOTE**
>
> 更新されたパッケージを使用するには、カーネルを再起動する必要があるかもしれません。

```python
import pandas as pd
from sklearn_evaluation import plot

# SQL セルを作成するために JupySQL Jupyter 拡張機能をインポートします。
%load_ext sql
%config SqlMagic.autocommit=False
```

**次のステージに進むためには、StarRocks インスタンスが起動してアクセス可能であることを確認する必要があります。**

> **NOTE**
>
> 接続しようとしているインスタンスタイプに応じて、接続文字列を調整する必要があります（url、ユーザー、およびパスワード）。以下の例では、ローカルインスタンスを使用しています。

## JupySQL を介して StarRocks に接続する

この例では、docker インスタンスが使用されており、それが接続文字列に反映されています。

`root` ユーザーを使用してローカルの StarRocks インスタンスに接続し、データベースを作成し、テーブルからデータを実際に読み書きできることを確認します。

```python
%sql mysql+pymysql://root:@localhost:9030
```

JupySQL データベースを作成して使用します：

```python
%sql CREATE DATABASE jupysql;
```

```python
%sql USE jupysql;
```

テーブルを作成します：

```python
%%sql
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 10), (2, 20), (3, 30);
SELECT * FROM tbl;
```

## クエリの保存とロード

データベースを作成した後、サンプルデータを書き込み、クエリを実行できます。

JupySQL を使用すると、クエリを複数のセルに分割でき、大規模なクエリを構築するプロセスが簡素化されます。

複雑なクエリを書いて保存し、必要に応じて実行することができます。これは SQL の CTE に似ています。

```python
# これは次の JupySQL リリースで保留中です。
%%sql --save initialize-table --no-execute
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
SELECT * FROM tbl;
```

> **NOTE**
>
> `--save` はクエリを保存しますが、データは保存しません。

`--with;` を使用していることに注意してください。これにより、以前に保存されたクエリが取得され、（CTE を使用して）前に追加されます。そして、クエリを `track_fav` に保存します。

## StarRocks での直接プロット

JupySQL にはデフォルトでいくつかのプロットが付属しており、SQL でデータを直接視覚化することができます。

新しく作成したテーブルのデータを視覚化するために、棒グラフを使用できます：

```python
top_artist = %sql SELECT * FROM tbl
top_artist.bar()
```

これで、追加のコードなしで新しい棒グラフが得られます。ノートブックから直接 JupySQL（ploomber による）を介して SQL を実行できます。これにより、データサイエンティストやエンジニアにとって StarRocks に関する多くの可能性が広がります。行き詰まったりサポートが必要な場合は、Slack を通じてご連絡ください。