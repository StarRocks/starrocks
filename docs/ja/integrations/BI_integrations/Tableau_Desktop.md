---
displayed_sidebar: docs
---

# Tableau

このトピックでは、StarRocks Tableau JDBC Connector を使用して StarRocks を Tableau Desktop および Tableau Server に接続する方法について説明します。

## 概要

StarRocks Tableau JDBC Connector は、Tableau Desktop および Tableau Server 用のカスタム拡張機能です。Tableau と StarRocks の接続プロセスを簡素化し、Tableau の標準機能のサポートを強化し、デフォルトの Generic ODBC/JDBC 接続を凌駕します。

### 主な機能

- LDAP サポート： 安全な認証のためのパスワードプロンプト付きの LDAP ログインを可能にします。
- 高い互換性： TDVT (Tableau Design Verification Tool) テストで 99.99% の互換性を達成。

## 前提条件

先に進む前に、以下の要件が満たされていることを確認してください：

- Tableau バージョン： Tableau 2020.4 以降
- StarRocks バージョン: v3.2 以降

## Tableau Desktop 用 Connector のインストール

1. [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/) をダウンロードします。
2. ドライバファイルを以下のディレクトリに保存します（ディレクトリが存在しない場合は作成します）：

   - macOS: `~/Library/Tableau/Drivers`
   - Windows: `C:\Program Files\Tableau\Drivers`

3. [StarRocks Tableau JDBC Connector file](https://releases.starrocks.io/resources/starrocks_jdbc-v1.2.0_signed.taco) をダウンロードします。
4. コネクタファイルを以下のディレクトリに保存します：

   - macOS: `~/Documents/My Tableau Repository/Connectors`
   - Windows: `C:\Users\[Windows User]\Documents\My Tableau Repository\Connectors`

5. Tableau Desktop を起動します。
6. **Connect** -> **To a Server** -> **StarRocks JDBC by CelerData** に移動します。

## Tableau Server 用 Connector のインストール

1. [MySQL JDBC Driver 8.0.33](https://downloads.mysql.com/archives/c-j/) をダウンロードします。
2. ドライバファイルを以下のディレクトリに保存します（ディレクトリが存在しない場合は作成します）：

   - Linux: `/opt/tableau/tableau_driver/jdbc`
   - Windows: `C:\Program Files\Tableau\Drivers`

   :::info

   Linux では、「Tableau」 ユーザがディレクトリにアクセスすることを許可する必要があります。

   以下の手順に従ってください：

   1. ディレクトリを作成し、ドライバファイルをディレクトリにコピーします：

      ```Bash
      sudo mkdir -p /opt/tableau/tableau_driver/jdbc

      # Replace <path_to_driver_file_name> with the absolute path of the driver file.
      sudo cp /<path_to_driver_file_name>.jar /opt/tableau/tableau_driver/jdbc
      ```
  
   2. Tableau "ユーザーにアクセス許可を与える。

      ```Bash
      # <driver_file_name> をドライバファイル名に置き換えてください。
      sudo chmod 755 /opt/tableau/tableau_driver/jdbc/<driver_file_name>.jar
      ```

   :::

3. [StarRocks Tableau JDBC Connector file](https://releases.starrocks.io/resources/starrocks_jdbc-v1.2.0_signed.taco) をダウンロードします。
4. 各ノードの以下のディレクトリにコネクタファイルを保存します：

   - Linux: `/opt/tableau/connectors`
   - Windows: `C:\Program Files\Tableau\Connectors`

5. Tableau Server を再起動します。

   ```Bash
   tsm restart
   ```

   :::info

   コネクタを追加、削除、または更新するたびに、変更を適用するために Tableau Server を再起動する必要があります。

   :::

## Usage notes

LDAP ログインのサポートが必要な場合は、設定中に **Advanced** タブの **Enable LDAP** スイッチにチェックを入れることができます。
