---
displayed_sidebar: docs
---

import FEConfigMethod from '../../_assets/commonMarkdown/FE_config_method.mdx'

import AdminSetFrontendNote from '../../_assets/commonMarkdown/FE_config_note.mdx'

import StaticFEConfigNote from '../../_assets/commonMarkdown/StaticFE_config_note.mdx'

import EditionSpecificFEItem from '../../_assets/commonMarkdown/Edition_Specific_FE_Item.mdx'

# FE 設定

<FEConfigMethod />

## FE 設定項目の表示

FE の起動後、MySQL クライアントで ADMIN SHOW FRONTEND CONFIG コマンドを実行して、パラメーター設定を確認できます。特定のパラメーターの設定をクエリするには、次のコマンドを実行します。

```SQL
ADMIN SHOW FRONTEND CONFIG [LIKE "pattern"];
```

返されるフィールドの詳細な説明については、[`ADMIN SHOW CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SHOW_CONFIG.md) を参照してください。

:::note
クラスター管理関連コマンドを実行するには、管理者権限が必要です。
:::

## FE パラメーターの設定

### FE 動的パラメーターの設定

[`ADMIN SET FRONTEND CONFIG`](../../sql-reference/sql-statements/cluster-management/config_vars/ADMIN_SET_CONFIG.md) を使用して、FE 動的パラメーターの設定を構成または変更できます。

```SQL
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

<AdminSetFrontendNote />

### FE 静的パラメーターの設定

<StaticFEConfigNote />

## パラメータグループ

パラメータは以下のカテゴリに分類されています：

- [ログ](./FE_parameters/log_server_meta.md)
- [サーバー](./FE_parameters/log_server_meta.md)
- [メタデータおよびクラスタ管理](./FE_parameters/log_server_meta.md)
- [ユーザー、ロール、および権限](./FE_parameters/user_query_loading.md)
- [クエリエンジン](./FE_parameters/user_query_loading.md)
- [ロードおよびアンロード](./FE_parameters/user_query_loading.md)
- [統計レポート](./FE_parameters/stats_storage.md)
- [ストレージ](./FE_parameters/stats_storage.md)
- [共有データ](./FE_parameters/shared_lake_other.md)
- [データレイク](./FE_parameters/shared_lake_other.md)
