---
displayed_sidebar: docs
description: "2 つの GEOGRAPHY 点間の球面距離をメートル単位で返します。"
---

# ST_DISTANCE

OGC:CRS84 のセマンティクスに基づき、2 つの `GEOGRAPHY` 点間の球面距離をメートル単位で返します。

## 構文

```SQL
DOUBLE ST_DISTANCE(GEOGRAPHY lhs, GEOGRAPHY rhs)
```

## 戻り値

両方の値が `POINT` である必要があります。いずれかの入力が `NULL` または EMPTY の場合は `NULL` を返します。`POINT` 以外のファミリー、サポートされていない次元、またはサポートされていないディスクリプターはエラーになります。

現在サポートされるのは `GEOGRAPHY` オーバーロードのみです。`GEOMETRY` 入力および他のジオメトリファミリーの組み合わせはサポートされません。

## 例

```SQL
SELECT ST_DISTANCE(
    ST_GeogFromText('POINT (0 0)'),
    ST_GeogFromText('POINT (1 0)'));
-- 約 111195.1 メートル
```

## キーワード

ST_DISTANCE,ST,DISTANCE,GEOGRAPHY
