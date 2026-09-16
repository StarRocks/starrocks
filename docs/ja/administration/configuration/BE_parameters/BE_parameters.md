---
displayed_sidebar: docs
description: "StarRocks BE の設定項目リファレンス：be.conf またはSQL で設定可能なすべての BE パラメーター。"
---

import BEConfigMethod from '../../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../../_assets/commonMarkdown/StaticBE_config_note.mdx'

import EditionSpecificBEItem from '../../../_assets/commonMarkdown/Edition_Specific_BE_Item.mdx'

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
- [ロギング](./log_server_meta.md)
- [サーバー](./log_server_meta.md)
- [メタデータおよびクラスタ管理](./log_server_meta.md)
- [クエリエンジン](./query_loading.md)
- [ロードおよびアンロード](./query_loading.md)
- [統計レポート](./stats_storage.md)
- [ストレージ](./stats_storage.md)
- [共有データ](./shared_lake_other.md)
- [データレイク](./shared_lake_other.md)
- [その他](./shared_lake_other.md)
