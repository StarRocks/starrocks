---
displayed_sidebar: docs
sidebar_label: "Alias"
---

# エイリアス

クエリ内でテーブル、カラム、またはカラムを含む式の名前を記述する際に、エイリアスを割り当てることができます。エイリアスは通常、元の名前よりも短く、覚えやすいものです。

エイリアスが必要な場合は、SELECTリストまたはFROMリスト内のテーブル、カラム、および式の名前の後にAS句を追加するだけです。ASキーワードはオプションです。ASを使用せずに、元の名前の直後にエイリアスを指定することもできます。

エイリアスまたはその他の識別子が、内部の [StarRocks keyword](../../keywords.md) と同じ名前を持つ場合、名前をバッククォートのペアで囲む必要があります。例：`rank`。

エイリアスは大文字と小文字を区別しますが、カラムエイリアスと式のエイリアスは大文字と小文字を区別しません。

例：

```sql
select tiny_column as name, int_column as sex from big_table;

select sum(tiny_column) as total_count from big_table;

select one.tiny_column, two.int_column from small_table one, <br/> big_table two where one.tiny_column = two.tiny_column;
```
