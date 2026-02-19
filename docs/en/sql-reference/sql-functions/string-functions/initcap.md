---
displayed_sidebar: docs
---

# initcap

- Converts the first letter of each word in a string to uppercase and the remaining characters to lowercase.
- This function is useful for normalizing text data, such as names, addresses, or titles, where consistent Title Casing is required.

## Syntax

```SQL
initcap(str)
```

## Parameters

- `str:` The string expression to be formatted. This can be a VARCHAR column, a literal string, or the result of another string function.

## Return value

Returns the formatted string as a VARCHAR.

> - Returns NULL if the argument is NULL.
> - Words are delimited by whitespace or non-alphanumeric characters (e.g., punctuation, symbols, numbers).
> - Characters immediately following a delimiter are uppercased; all other letters are lowercased.

## Examples

Example 1: Format basic strings (lowercase or uppercase).

```Plaintext
mysql> SELECT initcap('hello world');
+------------------------+
| initcap('hello world') |
+------------------------+
| Hello World            |
+------------------------+
```
```
mysql> SELECT initcap('HELLO WORLD');
+------------------------+
| initcap('HELLO WORLD') |
+------------------------+
| Hello World            |
+------------------------+
```

Example 2: Format mixed-case strings.

```Plaintext
mysql> SELECT initcap('sTaRroCks dAtAbAsE');
+-------------------------------+
| initcap('sTaRroCks dAtAbAsE') |
+-------------------------------+
| Starrocks Database            |
+-------------------------------+
```

Example 3: Handling delimiters (numbers, symbols, and punctuation).

```Plaintext
mysql> SELECT initcap('1st place, in-the-world!');
+-------------------------------------+
| initcap('1st place, in-the-world!') |
+-------------------------------------+
| 1st Place, In-The-World!            |
+-------------------------------------+
```

## Usage notes

**Word Boundaries:** The function identifies the start of a "word" based on any character that is not a letter or a number. For example, in `'abc-def'`, the hyphen - is a delimiter, so d becomes D.

**Numbers:** Numbers are treated as word characters if they appear at the start, but they do not change case. However, symbols following numbers will reset the word boundary (e.g., 1st -> 1st because 1 starts the word, but 1-st -> 1-St because - is a delimiter).