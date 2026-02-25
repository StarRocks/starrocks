---
description: Jupyter notebooks と StarRocks
displayed_sidebar: docs
---

# Jupyter

このガイドでは、ノートブック、コード、データのための最新のウェブベースのインタラクティブ開発環境である [Jupyter](https://jupyter.org/) と StarRocks クラスターを統合する方法を説明します。

これらはすべて、Jupyter 上で %sql、%%sql、%sqlplot マジックを介して SQL を実行し、大規模なデータセットをプロットできる [JupySQL](https://jupysql.ploomber.io/) によって可能になります。

JupySQL を Jupyter 上で使用して、StarRocks 上でクエリを実行することができます。

データがクラスターにロードされると、SQL プロットを通じてクエリと視覚化が可能になります。

## 前提条件

始める前に、以下のソフトウェアがローカルにインストールされている必要があります：

- [JupySQL](https://jupysql.ploomber.io/en/latest/quick-start.html): `pip install jupysql`
- Jupyterlab: `pip install jupyterlab`
- [SKlearn Evaluation](https://github.com/ploomber/sklearn-evaluation): `pip install sklearn-evaluation`
- Python
- pymysql: `pip install pymysql`

> **注意**
>
> 上記の要件を満たしたら、`jupyterlab` と呼び出すだけで Jupyter lab を開くことができます - これによりノートブックインターフェースが開きます。
> Jupyter lab がすでにノートブックで実行されている場合は、以下のセルを実行して依存関係を取得できます。

```python
# 必要なパッケージをインストールします。
%pip install --quiet jupysql sklearn-evaluation pymysql
```

> **注意**
>
> パッケージを更新した後、カーネルを再起動する必要があるかもしれません。

```python
import pandas as pd
from sklearn_evaluation import plot

# SQL セルを作成するために JupySQL Jupyter 拡張機能をインポートします。
%load_ext sql
%config SqlMagic.autocommit=False
```

**次のステージのために、StarRocks インスタンスが起動してアクセス可能であることを確認する必要があります。**

> **注意**
>
> 接続しようとしているインスタンスタイプ（url、ユーザー、パスワード）に応じて接続文字列を調整する必要があります。以下の例ではローカルインスタンスを使用しています。

## JupySQL を介して StarRocks に接続する

この例では、docker インスタンスが使用されており、それが接続文字列に反映されています。

`root` ユーザーを使用してローカルの StarRocks インスタンスに接続し、データベースを作成し、データがテーブルから実際に読み書きできることを確認します。

```python
%sql mysql+pymysql://root:@localhost:9030
```

その JupySQL データベースを作成して使用します：

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

データベースを作成した後、サンプルデータを書き込んでクエリを実行できます。

JupySQL はクエリを複数のセルに分割することを可能にし、大規模なクエリを構築するプロセスを簡素化します。

複雑なクエリを書いて保存し、必要に応じて実行することができます。これは SQL の CTE に似ています。

```python
# これは次の JupySQL リリースで保留中です。
%%sql --save initialize-table --no-execute
CREATE TABLE tbl(c1 int, c2 int) distributed by hash(c1) properties ("replication_num" = "1");
INSERT INTO tbl VALUES (1, 1), (2, 2), (3, 3);
SELECT * FROM tbl;
```

> **注意**
>
> `--save` はクエリを保存しますが、データは保存しません。

`--with;` を使用していることに注意してください。これにより、以前に保存されたクエリを取得し、それらを（CTE を使用して）前に追加します。そして、クエリを `track_fav` に保存します。

## StarRocks での直接プロット

JupySQL にはデフォルトでいくつかのプロットが含まれており、SQL でデータを直接視覚化することができます。

新しく作成したテーブルのデータを視覚化するために、棒グラフを使用できます：

```python
top_artist = %sql SELECT * FROM tbl
top_artist.bar()
```

これで、追加のコードなしで新しい棒グラフができました。JupySQL（ploomber による）を介してノートブックから直接 SQL を実行できます。これにより、データサイエンティストやエンジニアにとって StarRocks に関する多くの可能性が広がります。もし行き詰まったりサポートが必要な場合は、Slack を通じて私たちに連絡してください。