---
displayed_sidebar: docs
---

# INSTALL PLUGIN

## 説明

このステートメントは、プラグインをインストールするために使用されます。

:::tip

この操作には、SYSTEM レベルの PLUGIN 権限が必要です。この権限を付与するには、[GRANT](../../account-management/GRANT.md) の指示に従ってください。

:::

## 構文

```sql
INSTALL PLUGIN FROM [source] [PROPERTIES ("key"="value", ...)]
```

3 種類のソースがサポートされています:

```plain text
1. zip ファイルを指す絶対パス
2. プラグインディレクトリを指す絶対パス
3. zip ファイルを指す http または https ダウンロードリンク
```

PROPERTIES は、プラグインの設定を行うために使用されます。例えば、zip ファイルの md5sum 値を設定するなどです。

## 例

1. ローカルの zip ファイルからプラグインをインストールする:

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo.zip";
    ```

2. ローカルの inpath からプラグインをインストールする:

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo/";
    ```

3. プラグインをダウンロードしてインストールする:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
    ```

4. プラグインをダウンロードしてインストールし、同時に zip ファイルの md5sum 値を設定する:

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip" PROPERTIES("md5sum" = "73877f6029216f4314d712086a146570");
    ```