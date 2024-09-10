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
Files should end in `.thrift`.

```
my_struct.thrift            // Good
MyStruct.thrift             // Bad
my_struct.proto             // Bad
```

## struct name

Struct names start with a capital letter `T` and have a capital letter for each new word, with no underscores: TMyStruct

```
struct TMyStruct;           // Good
struct MyStruct;            // Bad
struct TMy_Struct;          // Bad
struct TmyStruct;           // Bad
```

## field name

The names of struct members are all lowercase, with underscores between words. 

```
1: optional i64 my_field;       // Good
1: optional i64 myField;        // Bad
```