---
displayed_sidebar: docs
---

# 規則

## required を使用しない

プロジェクトが進化するにつれて、フィールドは任意になる可能性があります。しかし、required と定義されている場合、それを削除することはできません。

したがって、`required` は使用しないでください。

## 順序を変更しない

後方互換性を保つために、フィールドの順序を変更してはいけません。

# 命名

## ファイル名

メッセージの名前はすべて小文字で、単語の間にアンダースコアを入れます。ファイルは `.thrift` で終わるべきです。

```
my_struct.thrift            // 良い
MyStruct.thrift             // 悪い
my_struct.proto             // 悪い
```

## 構造体名

構造体名は大文字の `T` で始まり、新しい単語ごとに大文字を使用し、アンダースコアは使用しません: TMyStruct

```
struct TMyStruct;           // 良い
struct MyStruct;            // 悪い
struct TMy_Struct;          // 悪い
struct TmyStruct;           // 悪い
```

## フィールド名

構造体メンバーの名前はすべて小文字で、単語の間にアンダースコアを入れます。

```
1: optional i64 my_field;       // 良い
1: optional i64 myField;        // 悪い
```