# json_length

## Description

 Returns the length of a JSON document. If a path is specified, this function returns the length of the value identified by the path.

The length of a document is determined according to the following rules:

- The length of a scalar value is 1. For example, the length of `1`, `"a"`, `true`, `false`, and `null` is 1.

- The length of an array is the number of array elements. For example, the length of `[1, 2]` is 2.

- The length of an object is the number of object members. For example, the length of `{"a": 1}` is 1.

- The length of nested arrays or objects does not count. For example, the length of `{"a": [1, 2]}` is 1, because the nested array `[1, 2]` is not calculated into length.

## Syntax

```Haskell
json_length(json_doc[, path])
```

## Parameters

`json_doc`: required, the JSON document for which to return the length.

`path`: optional. It is used to return the length of a value within the document. The path generally starts with `$` and uses `.` as the path separator. `[]` is used as the array subscript, which starts from 0.

## Return value

Returns a value of the INT type.

An error is returned if the JSON document is not a valid document.

0 is returned in any of the following scenarios:

- The **`path`** does not identify a value in the document. 

- The path is not a valid path expression.

- The path contains the `*` or `**` wildcard.

## Examples

Example 1: Return the length of a scalar value.

```Plain
select json_length('1');
+------------------+
| json_length('1') |
+------------------+
|                1 |
+------------------+
```

Example 2: Return the length of an empty object.

```Plain
select json_length('{}');
+-------------------+
| json_length('{}') |
+-------------------+
|                 0 |
+-------------------+
```

Example 3: Return the length of an object that has data.

```Plain
select json_length('{"Name": "Homer"}');
+----------------------------------+
| json_length('{"Name": "Homer"}') |
+----------------------------------+
|                                1 |
+----------------------------------+
```

Example 4: Return the length of a JSON array.

```plain text
select json_length('[1, 2, 3]');
+--------------------------+
| json_length('[1, 2, 3]') |
+--------------------------+
|                        3 |
+--------------------------+
```

Example 5: Return the length of a JSON array in which one element has a nested array.

The nested array `[3, 4]` is not calculated into length.

```plain text
select json_length('[1, 2, [3, 4]]');
+-------------------------------+
| json_length('[1, 2, [3, 4]]') |
+-------------------------------+
|                             3 |
+-------------------------------+
```

Example 6: Return the length of an object specified by path `$.Person`.

```SQL
SET @file = '{  
    "Person": {    
       "Name": "Homer", 
       "Age": 39,
       "Hobbies": ["Eating", "Sleeping"]  
    }
 }';
select json_length(@file, '$.Person') 'Result';
```

Example 7: Return the length of the value specified by path `$.y`.

```plain text
select json_length('{"x": 1, "y": [1, 2]}', '$.y');
+---------------------------------------------+
| json_length('{"x": 1, "y": [1, 2]}', '$.y') |
+---------------------------------------------+
|                                           2 |
+---------------------------------------------+
```
