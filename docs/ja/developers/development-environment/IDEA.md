---
displayed_sidebar: docs
---

# Setup StarRocks FE development environment on IDEA

このチュートリアルはmacOSに基づいており、Apple Chip(M1, M2)でテストされています。macOSを使用していなくても、このチュートリアルを参考にすることができます。

## Requirements

**Thrift 0.13**

brewで直接Thriftをインストールした場合、バージョン0.13がないことに気づくでしょう。以下のコマンドを使用できます。

```bash
brew tap-new $USER/local-tap
brew extract --version='0.13.0' thrift $USER/local-tap
brew install thrift@0.13.0
```

Thriftのインストールが成功したら、次のコマンドを実行して確認できます。

```bash
$ thrift -version
Thrift version 0.13.0
```

**Protobuf**

最新バージョンv3を使用してください。StarRocksで使用されているProtobufのv2バージョンと互換性があるためです。

```bash
brew install protobuf
```

**Maven**

```
brew install maven
```

**Openjdk 1.8 または 11**

```bash
brew install openjdk@11
```

**Python3**

MacOSにはデフォルトでインストールされています。

各自のThriftとProtobufのインストールディレクトリは異なる場合があります。brew listコマンドを使用して確認できます。

```bash
brew list thrift@0.13.0
brew list protobuf
```

## Configure the StarRocks

**StarRocksのダウンロード**

```
git clone https://github.com/StarRocks/starrocks.git
```

**thirdpartyディレクトリのセットアップ**

`thirdparty`に`installed/bin`ディレクトリを作成します。

```bash
cd starrocks && mkdir -p starrocks/thirdparty/installed/bin
```

次に、ThriftとProtobufのソフトリンクをそれぞれ作成します。

```bash
ln -s /opt/homebrew/bin/thrift thirdparty/installed/bin/thrift
ln -s /opt/homebrew/bin/protoc thirdparty/installed/bin/protoc
```

**環境変数の設定**

```bash
export JAVA_HOME="/opt/homebrew/Cellar/openjdk@11/11.0.15" (注意: jdkの安定バージョン番号は変更される可能性があります)
export PYTHON=/usr/bin/python3
export STARROCKS_THIRDPARTY=/Users/smith/Code/starrocks/thirdparty
```

## Generate source code

FEの多くのソースファイルは手動で生成する必要があります。そうしないと、IDEAがファイルの欠如によるエラーを報告します。次のコマンドを実行して自動的に生成します。

```bash
cd gensrc
make clean
make
```

## Compile FE

`fe`ディレクトリに入り、Mavenを使用してコンパイルします。

```bash
cd fe
mvn install -DskipTests
```

## Open StarRocks in IDEA

1. IDEAで`StarRocks`ディレクトリを開きます。

2. コーディングスタイル設定を追加する
   コーディングスタイルを標準化するために、IDEAで`fe/starrocks_intellij_style.xml`コードスタイルファイルをインポートする必要があります。

## Run StarRocks FE in MacOS

IDEAを使用して`fe`ディレクトリを開きます。

`StarRocksFE.java`でMain関数を直接実行すると、いくつかのエラーが報告されます。スムーズに実行するために、いくつかの簡単な設定を行うだけで済みます。

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

3. 環境変数を設定します。以下の図に示されています。

![image-20220701193938856](../../_assets/IDEA-1.png)

```bash
export PID_DIR=/Users/smith/Code/starrocks/fe/bin
export STARROCKS_HOME=/Users/smith/Code/starrocks/fe
export LOG_DIR=/Users/smith/Code/starrocks/fe/log
```

4. `fe/conf/fe.conf`のpriority_networksを`127.0.0.1/24`に変更して、FEが現在のコンピュータのLAN IPを使用してポートのバインドに失敗するのを防ぎます。

5. これでStarRocks FEを正常に実行できました。

![image-20220701193938856](../../_assets/IDEA-2.png)

## DEBUG StarRocks FE in MacOS

FEをデバッグオプションで起動した場合、IDEAデバッガをFEプロセスにアタッチできます。

```
./start_fe.sh --debug
```

https://www.jetbrains.com/help/idea/attaching-to-local-process.html#attach-to-local を参照してください。