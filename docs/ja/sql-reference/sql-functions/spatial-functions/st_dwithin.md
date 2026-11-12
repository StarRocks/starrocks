---
displayed_sidebar: docs
description: "点が線または複数線から指定された包含距離内にあるかを判定します。"
---

# ST_DWITHIN

`POINT` と `LINESTRING` または `MULTILINESTRING` の最小距離が、しきい値以下かを返します。2 つのジオメトリ引数はどちらの順序でも指定できます。

`GEOGRAPHY` は球面 OGC:CRS84 エッジで評価し、しきい値の単位はメートルです。`GEOMETRY` は平面エッジで評価し、しきい値には宣言された CRS の単位を使用します。`GEOMETRY` 入力には互換性のあるディスクリプタが必要です。この関数は CRS 変換を行いません。

## 構文

```SQL
BOOLEAN ST_DWITHIN(GEOGRAPHY point_or_line, GEOGRAPHY line_or_point, DOUBLE distance)
BOOLEAN ST_DWITHIN(GEOMETRY point_or_line, GEOMETRY line_or_point, DOUBLE distance)
```

## パラメータ

- ジオメトリ引数は `POINT`/`LINESTRING` または `POINT`/`MULTILINESTRING` の組み合わせである必要があります。
- `distance` は有限の非負値である必要があり、等しい場合も含みます。
- すべてのジオメトリ値は 2 次元である必要があります。

`GEOGRAPHY` の線にある正確な対蹠線分、または数値的に曖昧な対蹠線分は、一意な最短球面エッジを定義しないため拒否されます。

## 戻り値

ジオメトリ引数またはしきい値が `NULL` の場合は `NULL` を返します。ジオメトリ引数が EMPTY の場合は `false` を返します。未対応のファミリまたはディスクリプタ、不正なしきい値、`GEOGRAPHY` と `GEOMETRY` の混在、互換性のない `GEOMETRY` ディスクリプタはエラーになります。

## 例

```SQL
SELECT ST_DWITHIN(
    ST_GeogFromText('POINT (0 1)'),
    ST_GeogFromText('LINESTRING (-1 0, 1 0)'),
    111196);
-- true

SELECT ST_DWITHIN(
    ST_GeomFromText('POINT (5 3)', 'EPSG:3857'),
    ST_GeomFromText('LINESTRING (0 0, 10 0)', 'EPSG:3857'),
    3);
-- true
```

## キーワード

ST_DWITHIN,ST,DWITHIN,GEOGRAPHY,GEOMETRY
