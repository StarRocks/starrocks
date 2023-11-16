---
displayed_sidebar: "Chinese"
---

# from_days

## description

### Syntax

```Haskell
DATE FROM_DAYS(INT N)
```

通过距离0000-01-01日的天数计算出哪一天

## example

```Plain Text
MySQL > select from_days(730669);
+-------------------+
| from_days(730669) |
+-------------------+
| 2000-07-03        |
+-------------------+
```

## keyword

FROM_DAYS,FROM,DAYS
