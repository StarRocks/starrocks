---
displayed_sidebar: docs
---

# Ubuntu で StarRocks をコンパイルする

このトピックでは、Ubuntu オペレーティングシステムで StarRocks をコンパイルする方法について説明します。StarRocks は、x86_64 および AArch64 アーキテクチャの両方でのコンパイルをサポートしています。

## 前提条件

### 依存関係のインストール

必要な依存関係をインストールするには、次のコマンドを実行します。

```bash
sudo apt-get update
```

```
sudo apt-get install automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 bzip2 -y
```

### コンパイラのインストール

Ubuntu 22.04 以降を使用している場合は、次のコマンドを実行してツールとコンパイラをインストールします。

```bash
sudo apt-get install cmake gcc g++ default-jdk -y
```

Ubuntu 22.04 より前のバージョンを使用している場合は、次のコマンドを実行してツールとコンパイラのバージョンを確認します。

1. GCC/G++ のバージョンを確認:

   ```bash
   gcc --version
   g++ --version
   ```

   GCC/G++ のバージョンは 10.3 以上である必要があります。以前のバージョンを使用している場合は、[こちらをクリックして GCC/G++ をインストールしてください](https://gcc.gnu.org/releases.html)。

2. JDK のバージョンを確認:

   ```bash
   java --version
   ```

   OpenJDK のバージョンは 8 以上である必要があります。以前のバージョンを使用している場合は、[こちらをクリックして OpenJDK をインストールしてください](https://openjdk.org/install)。

3. CMake のバージョンを確認:

   ```bash
   cmake --version
   ```

   CMake のバージョンは 3.20.1 以上である必要があります。以前のバージョンを使用している場合は、[こちらをクリックして CMake をインストールしてください](https://cmake.org/download)。

## StarRocks のコンパイル

コンパイルを開始するには、次のコマンドを実行します。

```bash
./build.sh
```

デフォルトのコンパイル並行性は **CPU コア数/4** に等しいです。32 コアの CPU を持っていると仮定すると、デフォルトの並行性は 8 です。

並行性を調整したい場合は、コマンドラインで `-j` を使用してコンパイルに使用する CPU コアの数を指定できます。

次の例では、24 コアの CPU を使用してコンパイルします。

```bash
./build.sh -j 24
```

## FAQ

Q: Ubuntu 20.04 で `aws_cpp_sdk` のビルドが "Error: undefined reference to pthread_create" というエラーで失敗します。どうすれば解決できますか？

A: このエラーは、CMake のバージョンが低いために発生します。CMake をバージョン 3.20.1 以上にアップグレードしてください。