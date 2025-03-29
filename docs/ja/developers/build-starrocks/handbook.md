---
displayed_sidebar: docs
---

# How to build StarRocks

一般的に、StarRocks をビルドするには、以下のコマンドを実行します。

```
./build.sh
```

このコマンドは、まずすべてのサードパーティ依存関係が準備されているかを確認します。すべての依存関係が準備できていれば、StarRocks の `Backend` と `Frontend` をビルドします。

このコマンドが正常に実行されると、生成されたバイナリは `output` ディレクトリにあります。

## FE/BE を個別にビルドする

毎回 FE と BE の両方をビルドする必要はなく、個別にビルドすることができます。
例えば、BE のみをビルドするには
```
./build.sh --be
```

そして、FE のみをビルドするには
```
./build.sh --fe
```

# ユニットテストの実行方法

BE と FE のユニットテストは分かれています。一般的に、BE のテストを実行するには
```
./run-be-ut.sh
```

FE のテストを実行するには
```
./run-fe-ut.sh
```

## コマンドラインで BE UT を実行する方法

現在、BE UT を実行するにはいくつかの依存関係が必要で、`./run-be-ut.sh` がそれを助けます。しかし、これは十分に柔軟ではありません。コマンドラインで UT を実行したい場合は、以下を実行できます。

```
UDF_RUNTIME_DIR=./ STARROCKS_HOME=./ LD_LIBRARY_PATH=/usr/lib/jvm/java-18-openjdk-amd64/lib/server ./be/ut_build_ASAN/test/starrocks_test
```

StarRocks Backend UT は google-test の上に構築されているため、フィルタを渡して一部の UT を実行することができます。例えば、MapColumn に関連するテストのみを実行したい場合は、以下を実行します。

```
UDF_RUNTIME_DIR=./ STARROCKS_HOME=./ LD_LIBRARY_PATH=/usr/lib/jvm/java-18-openjdk-amd64/lib/server ./be/ut_build_ASAN/test/starrocks_test --gtest_filter="*MapColumn*"
```

# ビルドオプション

## clang でビルドする

`clang` を使って StarRocks をビルドすることもできます。

```
CC=clang CXX=clang++ ./build.sh --be
```

すると、ビルドメッセージに次のようなメッセージが表示されます。

```
-- compiler Clang version 14.0.0
```

## 異なるリンカでビルドする

デフォルトのリンカは遅いため、開発者は異なるリンカを指定してリンクを高速化できます。
例えば、LLVM ベースのリンカである `lld` を使用することができます。

まず `lld` をインストールする必要があります。

```
sudo apt install lld
```

次に、使用したいリンカを環境変数 STARROCKS_LINKER に設定します。
例えば：

```
STARROCKS_LINKER=lld ./build.sh --be
```

## 異なるタイプでビルドする

異なる BUILD_TYPE 変数を使用して、StarRocks を異なるタイプでビルドできます。デフォルトの BUILD_TYPE は `RELEASE` です。例えば、`ASAN` タイプで StarRocks をビルドするには

```
BUILD_TYPE=ASAN ./build.sh --be
```