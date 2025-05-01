---
displayed_sidebar: docs
---

# デバッグ用の debuginfo ファイルの使用

## 変更内容

v2.5 以降、BE の debuginfo ファイルはインストールパッケージのサイズとスペース使用量を削減するために StarRocks インストールパッケージから削除されました。 [StarRocks website](https://www.starrocks.io/download/community) で 2 つのパッケージを確認できます。

![debuginfo](../_assets/debug_info.png)

この図では、`Get Debug Symbol files` をクリックして debuginfo パッケージをダウンロードできます。`StarRocks-2.5.10.tar.gz` はインストールパッケージで、**Download** をクリックしてこのパッケージをダウンロードできます。

この変更は、StarRocks のダウンロードや使用に影響を与えません。クラスターのデプロイとアップグレードにはインストールパッケージのみをダウンロードできます。debuginfo パッケージは、開発者が GDB を使用してプログラムをデバッグするためのものです。

## 注意事項

デバッグには GDB 12.1 以降を推奨します。

## debuginfo ファイルの使用方法

1. debuginfo パッケージをダウンロードして解凍します。

    ```SQL
    wget https://releases.starrocks.io/starrocks/StarRocks-<sr_ver>.debuginfo.tar.gz

    tar -xzvf StarRocks-<sr_ver>.debuginfo.tar.gz
    ```

    > **NOTE**
    >
    > `<sr_ver>` をダウンロードしたい StarRocks インストールパッケージのバージョン番号に置き換えてください。

2. GDB デバッグを行う際に debuginfo ファイルをロードします。

    - **方法 1**

    ```Shell
    objcopy --add-gnu-debuglink=starrocks_be.debug starrocks_be
    ```

    この操作により、デバッグ情報ファイルが実行可能ファイルに関連付けられます。

    - **方法 2**

    ```Shell
    gdb -s starrocks_be.debug -e starrocks_be -c `core_file`
    ```

debuginfo ファイルは perf や pstack とも相性が良く、追加の操作なしで直接使用できます。