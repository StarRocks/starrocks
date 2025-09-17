---
displayed_sidebar: docs
---

# Tableau

このトピックでは、StarRocks を Tableau Desktop および Tableau Server に StarRocks Tableau JDBC Connector を使用して接続する方法について説明します。

## 概要

StarRocks Tableau JDBC Connector は、Tableau Desktop および Tableau Server 用のカスタム拡張機能です。Tableau を StarRocks に接続するプロセスを簡素化し、標準の Tableau 機能のサポートを強化し、デフォルトの Generic ODBC/JDBC 接続を上回ります。

### 主な機能

- LDAP サポート: パスワードプロンプトによる安全な認証を可能にする LDAP ログインをサポートします。
- 高い互換性: TDVT (Tableau Design Verification Tool) テストで 99.99% の互換性を達成し、わずかに失敗するケースが 1 つだけあります。

## 前提条件

次の要件を満たしていることを確認してください。

- Tableau バージョン: Tableau 2020.4 以降
- StarRocks バージョン: v3.2 以降

## Tableau Desktop 用コネクタのインストール

1. [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/) をダウンロードします。
2. ドライバファイルを次のディレクトリに保存します（ディレクトリが存在しない場合は作成してください）。

   - macOS: `~/Library/Tableau/Drivers`
   - Windows: `C:\Program Files\Tableau\Drivers`

3. [StarRocks Tableau JDBC Connector ファイル](https://exchange.tableau.com/products/1079) をダウンロードします。
4. コネクタファイルを次のディレクトリに保存します。

   - macOS: `~/Documents/My Tableau Repository/Connectors`
   - Windows: `C:\Users\[Windows User]\Documents\My Tableau Repository\Connectors`

5. Tableau Desktop を起動します。
6. **Connect** -> **To a Server** -> **StarRocks JDBC by CelerData** に移動します。

## Tableau Server 用コネクタのインストール

1. [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/) をダウンロードします。
2. ドライバファイルを次のディレクトリに保存します（ディレクトリが存在しない場合は作成してください）。

   - Linux: `/opt/tableau/tableau_driver/jdbc`
   - Windows: `C:\Program Files\Tableau\Drivers`

   :::info

   Linux では、"Tableau" ユーザーにディレクトリへのアクセスを許可する必要があります。

   次の手順に従ってください。

   1. ディレクトリを作成し、ドライバファイルをディレクトリにコピーします。

      ```Bash
      sudo mkdir -p /opt/tableau/tableau_driver/jdbc

      # <path_to_driver_file_name> をドライバファイルの絶対パスに置き換えてください。
      sudo cp /<path_to_driver_file_name>.jar /opt/tableau/tableau_driver/jdbc
      ```
  
   2. "Tableau" ユーザーに権限を付与します。

      ```Bash
      # <driver_file_name> をドライバファイルの名前に置き換えてください。
      sudo chmod 755 /opt/tableau/tableau_driver/jdbc/<driver_file_name>.jar
      ```

   :::

3. [StarRocks Tableau JDBC Connector ファイル](https://releases.starrocks.io/resources/starrocks_jdbc-v1.2.0_signed.taco) をダウンロードします。
4. 各ノードの次のディレクトリにコネクタファイルを保存します。

   - Linux: `/opt/tableau/connectors`
   - Windows: `C:\Program Files\Tableau\Connectors`

5. Tableau Server を再起動します。

   ```Bash
   tsm restart
   ```

   :::info

   コネクタを追加、削除、または更新するたびに、変更を適用するために Tableau Server を再起動する必要があります。

   :::

## 使用上の注意

LDAP ログインサポートが必要な場合は、設定中に **Advanced** タブで **Enable LDAP** スイッチをオンにすることができます。
