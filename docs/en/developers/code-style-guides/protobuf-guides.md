---
displayed_sidebar: docs
---

# Rules

## Never use required

As the project involving, any fields may become optional. But if it is defined as required, it can not be removed.

So `required` should not be used.

## Never change the ordinal

To be back compatible, the ordinal of the field SHOULD NOT be changed.

# Naming

## file name

The names of messages are all lowercase, with underscores between words.
Files should end in `.proto`.

```
my_message.proto            // Good
mymessage.proto             // Bad
my_message.pb                // Bad
```

## Message Name

Message names start with a capital letter and have a capital letter for each new word, with no underscores, and with `PB` as postfix: MyMessagePB

```protobuf
message MyMessagePB       // Good
message MyMessage         // Bad
message My_Message_PB     // Bad
message myMessagePB       // Bad
```

## field name

The names of messages are all lowercase, with underscores between words. 

```
optional int64 my_field = 3;        // Good
optional int64 myField = 3;         // Bad
```