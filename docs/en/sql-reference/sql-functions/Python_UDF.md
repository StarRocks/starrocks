---
displayed_sidebar: docs
sidebar_position: 0.9
---

# Python UDF

Since version 3.4.0, StarRocks supports writing User Defined Function (UDF) in Python.

This article describes how to write and use StarRocks Python UDFs.

Currently, StarRocks Python UDF only supports Scalar UDF.

## Prerequisites

Before using StarRocks' Java UDF functionality, you will need to.

- [Python3.8+ installed](https://www.python.org/downloads/release/python-380/) to run Python.

- The UDF feature is enabled. You can set the FE configuration item `enable_udf` to `true` in the FE configuration file **fe/conf/fe.conf** to enable this feature, and then restart the FE nodes to make the settings take effect. For more information, see [Parameter configuration](../../administration/management/FE_configuration.md).

- BE Set the Python interpreter environment variable location. Add the configuration item `python_envs` to set the location of the Python interpreter installation e.g. `/opt/Python-3.8/`.

## Develop and use UDFs
### Develop a scalar UDF
#### Syntax

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
[PROPERTIES ("key" = "value" [, ...]) | key="value" [...] ]
[AS $$ $$]
```
#### Create Python inline Scalar input UDF 
echo 示例

```SQL
CREATE FUNCTION python_echo(INT) RETURNS
INT
type = 'Python'
symbol = 'echo'
file = 'inline'
AS
$$
def echo(x):
    return x
$$
;
```

|parameters|description|
|---|----|
|symbol|UDF Execute function. |
|type| is used to mark the type of UDF created, in Python UDFs. Taking the value `Python` indicates a Python based UDF. |
|input|Type of input, takes the values `scalar` and `arrow`, defaults to `scalar`.|
#### Create Python inline vectorized input UDF 

In order to increase the speed of UDF processing, the vectorized input is provided.

```SQL
CREATE FUNCTION python_add(INT) RETURNS
INT
type = 'Python'
symbol = 'add'
input = "arrow"
AS
$$
import pyarrow.compute as pc
def add(x):
    return pc.add(x, 1)
$$
;
```
#### Create Python packaged input UDF 
Package Creation.
First, package the module into xxx.py.zip, which needs to meet the [zipimport format](https://docs.python.org/3/library/zipimport.html) .

```
> tree .
.
├── main.py
└── yaml
    ├── composer.py
    ├── constructor.py
    ├── cyaml.py
    ├── dumper.py
    ├── emitter.py
    ├── error.py
    ├── events.py
    ├── __init__.py
    ├── loader.py
    ├── nodes.py
    ├── parser.py
```
```
> cat main.py 
import numpy
import yaml

def echo(a):
    return yaml.__version__
```

```
CREATE FUNCTION py_pack(string) 
RETURNS  string 
symbol = "add"
type = "Python"
file = "http://HTTP_IP:HTTP_PORT/m1.py.zip"
symbol = "main.echo"
;
```
Note that the URL must end with .py.zip here.

## Mapping between SQL data types and Python data types

| SQL Type                             | Python 3 Type           |
| ------------------------------------ | ----------------------- |
| SCALAR:                              |                         |
| TINYINT/SMALLINT/INT/BIGINT/LARGEINT | INT                     |
| STRING                               | string                  |
| DOUBLE                               | FLOAT                   |
| BOOLEAN                              | BOOL                    |
| DATETIME                             | DATETIME.DATETIME       |
| FLOAT                                | FLOAT                   |
| CHAR                                 | STRING                  |
| VARCHAR                              | STRING                  |
| DATE                                 | DATETIME.DATE           |
| DECIMAL                              | DECIMAL.DECIMAL         |
| ARRAY                                | List                    |
| MAP                                  | Dict                    |
| STRUCT                               | COLLECTIONS.NAMEDTUPLE  |
| JSON                                 | dict                    |
| VECTORIZED:                          |                         |
| TYPE_BOOLEAN                         | pyarrow.lib.BoolArray   |
| TYPE_TINYINT                         | pyarrow.lib.Int8Array   |
| TYPE_SMALLINT                        | pyarrow.lib.Int15Array  |
| TYPE_INT                             | pyarrow.lib.Int32Array  |
| TYPE_BIGINT                          | pyarrow.lib.Int64Array  |
| TYPE_FLOAT                           | pyarrow.FloatArray      |
| TYPE_DOUBLE                          | pyarrow.DoubleArray     |
| VARCHAR                              | pyarrow.StringArray     |
| DECIMAL                              | pyarrow.Decimal128Array |
| DATE                                 | pyarrow.Date32Array     |
| TYPE_TIME                            | pyarrow.TimeArray       |
| ARRAY                                | pyarrow.ListArray       |