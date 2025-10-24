---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE 設定

<FEConfigMethod />

## FE の設定項目を表示する

FE が起動した後、MySQL クライアントで ADMIN SHOW FRONTEND CONFIG コマンドを実行してパラメータ設定を確認できます。特定のパラメータの設定を確認したい場合は、次のコマンドを実行してください。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[ADMIN SHOW CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を参照してください。

:::note
クラスタ管理関連のコマンドを実行するには、管理者権限が必要です。
:::

## FE パラメータを設定する

### FE 動的パラメータを設定する

[ADMIN SET FRONTEND CONFIG](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して FE 動的パラメータの設定を変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE 静的パラメータを設定する

<StaticFEConfigNote />

## FE パラメータを理解する

### ロギング

##### log_roll_size_mb

- デフォルト: 1024
- タイプ: Int
- 単位: MB
- 変更可能: いいえ
- 説明: システムログファイルまたは監査ログファイルの最大サイズ。
- 導入バージョン: -

### サーバー

### メタデータとクラスタ管理

### ユーザー、役割、特権

### クエリエンジン

### ロードとアンロード

### ストレージ

### 共有データ

### その他



<EditionSpecificFEItem />
