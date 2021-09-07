# ltrim

## description

### Syntax

```Haskell
VARCHAR ltrim(VARCHAR str)
```

将参数 str 中从开始部分连续出现的空格去掉

## example

```Plain Text
MySQL > SELECT ltrim('   ab d');
+------------------+
| ltrim('   ab d') |
+------------------+
| ab d             |
+------------------+
```

## keyword

LTRIM
