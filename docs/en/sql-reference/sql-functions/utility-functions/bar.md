---
displayed_sidebar: docs
---

# bar

Draw a bar graph like histogram to inspect the data distribution.

## Syntax

```SQL
bar(size, min, max, width) 
```

## Parameters

- `size`: size of the bar, must be within the `[min, max]`
- `min`: min value of the bar
- `max`: max value of the bar
- `width`: width of the bar


## Example

```SQL
MYSQL > select r, bar(r, 0, 10, 20) as x from table(generate_series(0, 10)) as s(r);

0	
1	▓▓
2	▓▓▓▓
3	▓▓▓▓▓▓
4	▓▓▓▓▓▓▓▓
5	▓▓▓▓▓▓▓▓▓▓
6	▓▓▓▓▓▓▓▓▓▓▓▓
7	▓▓▓▓▓▓▓▓▓▓▓▓▓▓
8	▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
9	▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓
10	▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓

```
