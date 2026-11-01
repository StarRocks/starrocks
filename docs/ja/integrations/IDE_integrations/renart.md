---
sidebar_position: 60
displayed_sidebar: docs
description: オープンソースのデータプラットフォーム Renart を StarRocks に接続し、Git ベースのローカルワークスペースからテーブルをクエリします。
---

# Renart

[Renart](https://github.com/renart-data/renart) は、データベースやデータウェアハウスをまたいでデータを移動、変換、分析するためのオープンソースのデータプラットフォームです。SQL と Python のパイプライン、ノートブック、ダッシュボードをローカルワークスペースにまとめ、型チェックとスケジュール実行を提供し、各定義をコードとして Git に保存します。このガイドでは、StarRocks データベースへの接続、テーブルと列の参照、行のプレビュー、クエリの実行を説明します。

## 前提条件

- [Renart インストールガイド](https://getrenart.com/docs/installation/)に従ってインストールした Renart v0.5.5。
- Renart ワークスペースとして開くローカル Git リポジトリ。
- 稼働中の StarRocks クラスター、FE のホスト名とクエリポート、および対象データベースをクエリする権限を持つ認証情報。

接続とクエリの手順は Renart v0.5.5 と StarRocks 3.5.21 でテストしました。

## StarRocks への接続

1. ローカル Git リポジトリ内で `renart web` を実行して Renart を開きます。
2. **Build → Connections** を開き、接続を追加して **StarRocks** を選択します。
3. 接続名（例：`starrocks-analytics`）と次の値を入力します。

   | フィールド | 値 |
   | --- | --- |
   | Host | StarRocks FE のホスト名または IP アドレス |
   | Port | FE のクエリポート。通常は `9030` |
   | Database | クエリするデータベース |
   | Username | StarRocks のユーザー名 |
   | Password | そのユーザーのパスワード |

4. パスワードの認証情報ソースを選択します。Renart は OS の認証情報ストア、暗号化されたローカル保管庫、または環境変数へのバインドをサポートします。[Renart の接続認証情報](https://getrenart.com/docs/connections-environments/managing-credentials/)を参照してください。
5. クエリ専用の接続では **Access → Read-only** を選択します。
6. **Verify** をクリックし、対象の環境に接続を保存します。

:::note
読み取り専用アクセスは、Renart が管理する操作を制限します。データベース側でも適切な権限を持つアカウントを使用してください。書き込み可能な接続では、アドホック SQL クエリで書き込み操作を実行できます。
:::

## テーブルと列の参照

Renart で **Data Browser** を開き、保存した StarRocks 接続から対象のデータベースに移動します。テーブルを選択すると、列名とデータベースの型を確認し、行をプレビューできます。以下の SQL クエリでも同じ接続と環境を使用します。

## クエリの実行

Renart の SQL クエリワークスペースで、保存した接続と環境を選択します。まず、接続先の StarRocks バージョンを確認します。

```sql
SELECT current_version();
```

次に、アカウントが読み取れるテーブルをクエリします。例のデータベース名とテーブル名を実際の名前に置き換えてください。

```sql
SELECT * FROM my_database.my_table LIMIT 100;
```

大きなテーブルを調べる際は、SQL に明示的な行数制限を設定してください。表示する結果の上限は、データベースが実行する処理量を必ずしも制限しません。

## トラブルシューティング

- 接続の検証に失敗した場合は、FE への到達性、クエリポート、データベース名、ユーザー権限を確認してください。クエリポートは HTTP ロード用のエンドポイントとは異なります。
- テーブルのマテリアライズとデータロードには、書き込み可能な接続と個別の設定が必要です。このクエリガイドでは Stream Load を設定しません。

Renart の問題は [Renart リポジトリ](https://github.com/renart-data/renart/issues)に報告してください。
