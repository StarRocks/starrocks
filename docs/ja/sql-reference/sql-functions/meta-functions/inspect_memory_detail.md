---
displayed_sidebar: docs
---

# inspect_memory_detail

`inspect_memory_detail(module_name, class_info)`

この関数は、モジュール内の特定のクラスまたはフィールドの推定メモリ使用量を返します。

## 引数

`module_name`: モジュールの名前 (VARCHAR)。
`class_info`: クラスの名前、または「class_name.field_name」 (VARCHAR)。

## 戻り値

推定メモリサイズを表す VARCHAR 文字列を返します（例: 「100MB」）。

