---
displayed_sidebar: docs
---

import BEConfigMethod from '../../_assets/commonMarkdown/BE_config_method.mdx'

import CNConfigMethod from '../../_assets/commonMarkdown/CN_config_method.mdx'

import PostBEConfig from '../../_assets/commonMarkdown/BE_dynamic_note.mdx'

import StaticBEConfigNote from '../../_assets/commonMarkdown/StaticBE_config_note.mdx'

# BE 設定

<BEConfigMethod />

<CNConfigMethod />

## BE の設定項目を表示する

次のコマンドを使用して BE の設定項目を表示できます。

```shell
curl http://<BE_IP>:<BE_HTTP_PORT>/varz
```

## BE パラメータを設定する

<PostBEConfig />

<StaticBEConfigNote />

## BE パラメータを理解する

### ロギング

### サーバー

##### be_port

- デフォルト: 9060
- タイプ: Int
- 単位: -
- 可変: いいえ
- 説明: FE からのリクエストを受け付ける BE thrift server のポート。
- 導入バージョン: -

### クエリエンジン

### ロード

### 統計レポート

### ストレージ

### 共有データ

### データレイク

### その他

