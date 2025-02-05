---
displayed_sidebar: docs
sidebar_position: 0.91
---

# Python UDF

This topic describes how to develop User Defined Functions (UDF) using Python.

Since v3.4.0, StarRocks supports creating Python UDFs.

Currently, StarRocks only supports scalar UDFs in Python.

## Prerequisites

Make sure the following requirements are met before proceeding:

- Install [Python 3.8](https://www.python.org/downloads/release/python-380/) or later.
- Enable UDF in StarRocks by setting  `enable_udf` to `true` in the FE configuration file **fe/conf/fe.conf**, and then restart the FE nodes to allow the configuration to take effect. For more information, see [FE configuration - enable_udf](../../administration/management/FE_configuration.md#enable_udf).
- Set the location of the Python interpreter environment in the BE instance using environment variable. Add the variable item `python_envs` and set it to the location of the Python interpreter installation, for example, `/opt/Python-3.8/`.

## Develop and use Python UDFs

Syntax:

```SQL
CREATE [GLOBAL] FUNCTION function_name(arg_type [, ...])
RETURNS return_type
{PROPERTIES ("key" = "value" [, ...]) | key="value" [...] }
[AS $$ $$]
```

| **Parameter**      | **Required** | **Description**                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| GLOBAL        | No       | Whether to create a global UDF.                              |
| function_name | Yes      | The name of the function you want to create. You can include the name of the database in this parameter, for example,`db1.my_func`. If `function_name` includes the database name, the UDF is created in that database. Otherwise, the UDF is created in the current database. The name of the new function and its parameters cannot be the same as an existing name in the destination database. Otherwise, the function cannot be created. The creation succeeds if the function name is the same but the parameters are different. |
| arg_type      | Yes      | Argument type of the function. The added argument can be represented by `, ...`. For the supported data types, see [Mapping between SQL data types and Python data types](#mapping-between-sql-data-types-and-python-data-types). |
| return_type   | Yes      | The return type of the function. For the supported data types, see [Mapping between SQL data types and Python data types](#mapping-between-sql-data-types-and-python-data-types). |
| PROPERTIES    | Yes      | Properties of the function, which vary depending on the type of the UDF to create. |
| AS $$ $$      | No       | Specify the inline UDF code between `$$` marks.              |

Properties include:

| **Property**  | **Required** | **Description**                                                  |
| ------------- | ------------ | ---------------------------------------------------------------- |
| type          | Yes          | The type of the UDF. Setting it to `Python` indicates to create a Python-based UDF. |
| symbol        | Yes          | The name of the class for the Python project to which the UDF belongs. The value of this parameter is in the `<package_name>.<class_name>` format. |
| input         | No           | Type of input. Valid values: `scalar` (Default) and `arrow` (vectorized input).     |
| file          | No           | The HTTP URL from which you can download the Python package file that contains the code for the UDF. The value of this parameter is in the `http://<http_server_ip>:<http_server_port>/<python_package_name>` format. Note the package name must have the suffix `.py.zip`. Default value: `inline`, which indicates to create an inline UDF. |

### Create an inline scalar input UDF using Python

The following example creates an inline `echo` function with scalar input using Python.

```SQL
CREATE FUNCTION python_echo(INT)
RETURNS INT
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

### Create an inline vectorized input UDF using Python

Vectorized input is supported to enhance the UDF processing performance.

The following example creates an inline `add` function with vectorized input using Python.

```SQL
CREATE FUNCTION python_add(INT) 
RETURNS INT
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

### Create a packaged UDF using Python

When creating the Python package, you must package the module into `.py.zip` files, which need to meet the [zipimport format](https://docs.python.org/3/library/zipimport.html).

```Plain
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

```Plain
> cat main.py 
import numpy
import yaml

def echo(a):
    return yaml.__version__
```

```SQL
CREATE FUNCTION py_pack(string) 
RETURNS  string 
symbol = "add"
type = "Python"
file = "http://HTTP_IP:HTTP_PORT/m1.py.zip"
symbol = "main.echo"
;
```

## Mapping between SQL data types and Python data types

| SQL Type                             | Python 3 Type           |
| ------------------------------------ | ----------------------- |
| **SCALAR**                           |                         |
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
| **VECTORIZED**                       |                         |
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
