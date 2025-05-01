---
displayed_sidebar: docs
---

# INSTALL PLUGIN

## 説明

このステートメントはプラグインをインストールするために使用されます。

構文:

```sql
INSTALL PLUGIN FROM [source] [PROPERTIES ("key"="value", ...)]
```

3 種類のソースがサポートされています:

```plain text
1. zip ファイルを指す絶対パス
2. プラグインディレクトリを指す絶対パス
3. zip ファイルを指す http または https ダウンロードリンク
```

PROPERTIES はプラグインの設定をサポートしており、例えば zip ファイルの md5sum 値を設定することができます。

## 例

1. ローカルの zip ファイルからプラグインをインストール:

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo.zip";
    ```

2. ローカルのインパスからプラグインをインストール:

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo/";
    ```

3. プラグインをダウンロードしてインストール:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
    ```

4. プラグインをダウンロードしてインストール。同時に、zip ファイルの md5sum 値を設定:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip" PROPERTIES("md5sum" = "73877f6029216f4314d712086a146570");
    ```