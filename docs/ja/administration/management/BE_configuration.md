---
displayed_sidebar: docs
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

# BE 設定

<BEConfigMethod />

<CNConfigMethod />

## BE の設定項目を表示する

次のコマンドを使用して BE の設定項目を表示できます。

```SQL
SELECT * FROM information_schema.be_configs WHERE NAME LIKE "%<name_pattern>%"
```

## BE パラメータを設定する

<PostBEConfig />

<StaticBEConfigNote />

## パラメータグループ

パラメータは以下のカテゴリに分類されています：
- [ロギング](./BE_parameters/log_server_meta.md)
- [サーバー](./BE_parameters/log_server_meta.md)
- [メタデータおよびクラスタ管理](./BE_parameters/log_server_meta.md)
- [クエリエンジン](./BE_parameters/query_loading.md)
- [ロードおよびアンロード](./BE_parameters/query_loading.md)
- [統計レポート](./BE_parameters/stats_storage.md)
- [ストレージ](./BE_parameters/stats_storage.md)
- [共有データ](./BE_parameters/shared_lake_other.md)
- [データレイク](./BE_parameters/shared_lake_other.md)
- [その他](./BE_parameters/shared_lake_other.md)
