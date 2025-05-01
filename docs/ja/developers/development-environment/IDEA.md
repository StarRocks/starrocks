---
displayed_sidebar: docs
---

# Setup StarRocks FE development environment on IDEA

このチュートリアルはmacOSに基づいており、Apple Chip(M1, M2)でテストされています。macOSを使用していない場合でも、このチュートリアルを参考にできます。

## 要件

### Thrift 0.13

公式のbrewリポジトリにはThriftの0.13バージョンがありません。私たちのコミッターの一人がインストール用にリポジトリにバージョンを作成しました。

```bash
brew install alberttwong/thrift/thrift@0.13
```

Thriftを正常にインストールした後、次のコマンドを実行して確認できます。

```bash
$ thrift -version
Thrift version 0.13.0
```

### Protobuf

最新バージョンv3を使用してください。StarRocksで使用されているProtobufのv2バージョンと互換性があるためです。

```bash
brew install protobuf
```

### Maven

```
brew install maven
```

### Openjdk 1.8または11

```bash
brew install openjdk@11
```

### Python3

MacOSにはデフォルトでインストールされています。

ThriftとProtobufのインストールディレクトリは人それぞれ異なる場合があります。brew listコマンドを使用して確認できます。

```bash
brew list thrift@0.13.0
brew list protobuf
```

## StarRocksの設定

### StarRocksのダウンロード

```
git clone https://github.com/StarRocks/starrocks.git
```

### thirdpartyディレクトリの設定

`thirdparty`に`installed/bin`ディレクトリを作成します。

```bash
cd starrocks && mkdir -p thirdparty/installed/bin
```

次に、ThriftとProtobufのソフトリンクをそれぞれ作成します。

```bash
ln -s /opt/homebrew/bin/thrift thirdparty/installed/bin/thrift
ln -s /opt/homebrew/bin/protoc thirdparty/installed/bin/protoc
```

### 環境変数の設定

```bash
export JAVA_HOME="/opt/homebrew/Cellar/openjdk@11/11.0.15" # 注意: jdkバージョンはデスクトップによって異なる場合があります
export PYTHON=/usr/bin/python3
export STARROCKS_THIRDPARTY=$(pwd)/thirdparty # 注意: starrocksディレクトリにいることを確認してください
```

## ソースコードの生成

FEの多くのソースファイルは手動で生成する必要があります。そうしないと、IDEAはファイルが見つからないためエラーを報告します。次のコマンドを実行して自動的に生成します。

```bash
cd gensrc
make clean
make
```

## FEのコンパイル

`fe`ディレクトリに入り、Mavenを使用してコンパイルします。

```bash
cd fe
mvn install -DskipTests
```

## IDEAでStarRocksを開く

1. IDEAで`StarRocks`ディレクトリを開きます。

2. コーディングスタイル設定を追加
    コーディングスタイルを標準化するために、IDEAで`fe/starrocks_intellij_style.xml`コードスタイルファイルをインポートする必要があります。
![image-20220701193938856](../../_assets/IDEA-2.png)

## MacOSでStarRocks FEを実行

IDEAを使用して`fe`ディレクトリを開きます。

`StarRocksFE.java`でMain関数を直接実行すると、いくつかのエラーが報告されます。スムーズに実行するためには、いくつかの簡単な設定を行うだけで済みます。

**注意:** `StarRocksFE.java`は`fe/fe-core/src/main/java/com/starrocks`ディレクトリにあります。

1. StarRocksディレクトリから`fe`ディレクトリにconf、bin、webrootディレクトリをコピーします。

```bash
cp -r conf fe/conf
cp -r bin fe/bin
cp -r webroot fe/webroot
```

2. `fe`ディレクトリに入り、`fe`ディレクトリの下にlogとmetaフォルダを作成します。

```bash
cd fe
mkdir log
mkdir meta
```

3. 環境変数を設定します。以下の図のように設定します。

![image-20220701193938856](../../_assets/IDEA-1.png)

```bash
export PID_DIR=/Users/smith/Code/starrocks/fe/bin
export STARROCKS_HOME=/Users/smith/Code/starrocks/fe
export LOG_DIR=/Users/smith/Code/starrocks/fe/log
```

4. `fe/conf/fe.conf`のpriority_networksを`127.0.0.1/24`に変更して、FEが現在のコンピュータのLAN IPを使用し、ポートがバインドできなくなるのを防ぎます。

5. これでStarRocks FEを正常に実行できました。

## MacOSでStarRocks FEをデバッグ

FEをデバッグオプションで開始した場合、IDEAデバッガをFEプロセスにアタッチできます。

```
./start_fe.sh --debug
```

https://www.jetbrains.com/help/idea/attaching-to-local-process.html#attach-to-local を参照してください。