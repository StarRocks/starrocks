---
displayed_sidebar: docs
---

# Protobuf ガイド

## required を使用しない

プロジェクトが進化するにつれて、フィールドは任意になる可能性があります。しかし、required として定義されている場合、それを削除することはできません。

したがって、`required` は使用しないでください。

## 順序を変更しない

互換性を保つために、フィールドの順序を変更してはいけません。

# 命名規則

## ファイル名

メッセージの名前はすべて小文字で、単語の間にアンダースコアを入れます。ファイルは `.proto` で終わるべきです。

```
my_message.proto            // 良い
mymessage.proto             // 悪い
my_message.pb               // 悪い
```

## メッセージ名

メッセージ名は大文字で始まり、新しい単語ごとに大文字を使用し、アンダースコアを使用せず、`PB` を接尾辞として付けます: MyMessagePB

```protobuf
message MyMessagePB       // 良い
message MyMessage         // 悪い
message My_Message_PB     // 悪い
message myMessagePB       // 悪い
```

## フィールド名

メッセージの名前はすべて小文字で、単語の間にアンダースコアを入れます。

```
optional int64 my_field = 3;        // 良い
optional int64 myField = 3;         // 悪い
```