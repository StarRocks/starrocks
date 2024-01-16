---
displayed_sidebar: "English"
---

# Java UDFs

From v2.2.0 onwards, you can compile user-defined functions (UDFs) to suit your specific business needs by using the Java programming language.

This topic describes the types of UDFs supported by StarRocks, how to develop UDFs, and how to use the UDFs you have developed.

Currently, StarRocks supports scalar UDFs, user-defined aggregate functions (UDAFs), user-defined window functions (UDWFs), and user-defined table functions (UDTFs).

## Prerequisites

- You have installed [Apache Maven](https://maven.apache.org/download.cgi), so you can create and compile Java projects.

- You have installed JDK 1.8 on your servers.

- The Java UDF feature is enabled. You can set the FE configuration item `enable_udf` to `true` in the FE configuration file **fe/conf/fe.conf** to enable this feature, and then restart the FE nodes to make the settings take effect. For more information, see [Parameter configuration](../../administration/Configuration.md).

## Develop and use UDFs

You need to create a Maven project and compile the UDF you need by using the Java programming language.

### Step 1: Create a Maven project

Create a Maven project, whose basic directory structure is as follows:

```Plain
project
|--pom.xml
|--src
|  |--main
|  |  |--java
|  |  |--resources
|  |--test
|--target
```

### Step 2: Add dependencies

Add the following dependencies to the **pom.xml** file:

```XML
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>org.example</groupId>
    <artifactId>udf</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>8</maven.compiler.source>
        <maven.compiler.target>8</maven.compiler.target>
    </properties>

    <dependencies>
        <dependency>
            <groupId>com.alibaba</groupId>
            <artifactId>fastjson</artifactId>
            <version>1.2.76</version>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-dependency-plugin</artifactId>
                <version>2.10</version>
                <executions>
                    <execution>
                        <id>copy-dependencies</id>
                        <phase>package</phase>
                        <goals>
                            <goal>copy-dependencies</goal>
                        </goals>
                        <configuration>
                            <outputDirectory>${project.build.directory}/lib</outputDirectory>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-assembly-plugin</artifactId>
                <version>3.3.0</version>
                <executions>
                    <execution>
                        <id>make-assembly</id>
                        <phase>package</phase>
                        <goals>
                            <goal>single</goal>
                        </goals>
                    </execution>
                </executions>
                <configuration>
                    <descriptorRefs>
                        <descriptorRef>jar-with-dependencies</descriptorRef>
                    </descriptorRefs>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### Step 3: Compile a UDF

Use the Java programming language to compile a UDF.

#### Compile a scalar UDF

A scalar UDF operates on a single row of data and returns a single value. When you use a scalar UDF in a query, each row corresponds to a single value in the result set. Typical scalar functions include `UPPER`, `LOWER`, `ROUND`, and `ABS`.

Suppose that the values of a field in your JSON data are JSON strings rather than JSON objects. When you use an SQL statement to extract JSON strings, you need to run `GET_JSON_STRING` twice, for example, `GET_JSON_STRING(GET_JSON_STRING('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key"), "$.k0")`.

To simplify the SQL statement, you can compile a scalar UDF that can directly extract JSON strings, for example, `MY_UDF_JSON_GET('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key.k0")`.

```Java
package com.starrocks.udf.sample;

import com.alibaba.fastjson.JSONPath;

public class UDFJsonGet {
    public final String evaluate(String obj, String key) {
        if (obj == null || key == null) return null;
        try {
            // The JSONPath library can be fully expanded even if the values of a field are JSON strings.
            return JSONPath.read(obj, key).toString();
        } catch (Exception e) {
            return null;
        }
    }
}
```

The user-defined class must implement the method described in the following table.

> **NOTE**
>
> The data types of the request parameters and return parameters in the method must be the same as those declared in the `CREATE FUNCTION` statement that is to be executed in [Step 6](#step-6-create-the-udf-in-starrocks) and conform to the mapping that is provided in the "[Mapping between SQL data types and Java data types](#mapping-between-sql-data-types-and-java-data-types)" section of this topic.

| Method                     | Description                                                  |
| -------------------------- | ------------------------------------------------------------ |
| TYPE1 evaluate(TYPE2, ...) | Runs the UDF. The evaluate() method requires the public member access level. |

#### Compile a UDAF

A UDAF operates on multiple rows of data and returns a single value. Typical aggregate functions include `SUM`, `COUNT`, `MAX`, and `MIN`, which aggregate multiple rows of data specified in each GROUP BY clause and return a single value.

Suppose that you want to compile a UDAF named `MY_SUM_INT`. Unlike the built-in aggregate function `SUM`, which returns BIGINT-type values, the `MY_SUM_INT` function supports only request parameters and returns parameters of the INT data type.

```Java
package com.starrocks.udf.sample;

public class SumInt {
    public static class State {
        int counter = 0;
        public int serializeLength() { return 4; }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {
    }

    public final void update(State state, Integer val) {
        if (val != null) {
            state.counter+= val;
        }
    }

    public void serialize(State state, java.nio.ByteBuffer buff) {
        buff.putInt(state.counter);
    }

    public void merge(State state, java.nio.ByteBuffer buffer) {
        int val = buffer.getInt();
        state.counter += val;
    }

    public Integer finalize(State state) {
        return state.counter;
    }
}
```

The user-defined class must implement the methods described in the following table.

> **NOTE**
>
> The data types of the request parameters and return parameters in the methods must be the same as those declared in the `CREATE FUNCTION` statement that is to be executed in [Step 6](#step-6-create-the-udf-in-starrocks) and conform to the mapping that is provided in the "[Mapping between SQL data types and Java data types](#mapping-between-sql-data-types-and-java-data-types)" section of this topic.

| Method                            | Description                                                  |
| --------------------------------- | ------------------------------------------------------------ |
| State create()                    | Creates a state.                                             |
| void destroy(State)               | Destroys a state.                                            |
| void update(State, ...)           | Updates a state. In addition to the first parameter `State`, you can also specify one or more request parameters in the UDF declaration. |
| void serialize(State, ByteBuffer) | Serializes a state into the byte buffer.                     |
| void merge(State, ByteBuffer)     | Deserializes a state from the byte buffer, and merges the byte buffer into the state as the first parameter. |
| TYPE finalize(State)              | Obtains the final result of the UDF from a state.            |

During compilation, you must also use the buffer class `java.nio.ByteBuffer` and the local variable `serializeLength`, which are described in the following table.

| Class and local variable | Description                                                  |
| ------------------------ | ------------------------------------------------------------ |
| java.nio.ByteBuffer()    | The buffer class, which stores intermediate results. Intermediate results may be serialized or deserialized when they are transmitted between nodes for execution. Therefore, you must also use the `serializeLength` variable to specify the length that is allowed for the deserialization of intermediate results. |
| serializeLength()        | The length that is allowed for the deserialization of intermediate results. Unit: bytes. Set this local variable to an INT-type value. For example, `State { int counter = 0; public int serializeLength() { return 4; }}` specifies that intermediate results are of the INT data type and the length for deserialization is 4 bytes. You can adjust these settings based on your business requirements. For example, if you want to specify the data type of intermediate results as LONG and the length for deserialization as 8 bytes, pass `State { long counter = 0; public int serializeLength() { return 8; }}`. |

Take note of the following points for the deserialization of intermediate results stored in the `java.nio.ByteBuffer` class:

- The `remaining()` method that is dependent on the `ByteBuffer` class cannot be called to deserialize a state.
- The `clear()` method cannot be called on the `ByteBuffer` class.
- The value of `serializeLength` must be the same as the length of the written-in data. Otherwise, incorrect results are generated during serialization and deserialization.

#### Compile a UDWF

Unlike regular aggregate functions, a UDWF operates on a set of multiple rows, which are collectively called a window, and returns a value for each row. A typical window function includes an `OVER` clause that divides rows into multiple sets. It performs a calculation across each set of rows and returns a value for each row.

Suppose that you want to compile a UDWF named `MY_WINDOW_SUM_INT`. Unlike the built-in aggregate function `SUM`, which returns BIGINT-type values, the `MY_WINDOW_SUM_INT` function supports only request parameters and returns parameters of the INT data type.

```Java
package com.starrocks.udf.sample;

public class WindowSumInt {    
    public static class State {
        int counter = 0;
        public int serializeLength() { return 4; }
        @Override
        public String toString() {
            return "State{" +
                    "counter=" + counter +
                    '}';
        }
    }

    public State create() {
        return new State();
    }

    public void destroy(State state) {

    }

    public void update(State state, Integer val) {
        if (val != null) {
            state.counter+=val;
        }
    }

    public void serialize(State state, java.nio.ByteBuffer buff) {
        buff.putInt(state.counter);
    }

    public void merge(State state, java.nio.ByteBuffer buffer) {
        int val = buffer.getInt();
        state.counter += val;
    }

    public Integer finalize(State state) {
        return state.counter;
    }

    public void reset(State state) {
        state.counter = 0;
    }

    public void windowUpdate(State state,
                            int peer_group_start, int peer_group_end,
                            int frame_start, int frame_end,
                            Integer[] inputs) {
        for (int i = (int)frame_start; i < (int)frame_end; ++i) {
            state.counter += inputs[i];
        }
    }
}
```

The user-defined class must implement the method required by UDAFs (because a UDWF is a special aggregate function) and the `windowUpdate()` method described in the following table.

> **NOTE**
>
> The data types of the request parameters and return parameters in the method must be the same as those declared in the `CREATE FUNCTION` statement that is to be executed in [Step 6](#step-6-create-the-udf-in-starrocks) and conform to the mapping that is provided in the "[Mapping between SQL data types and Java data types](#mapping-between-sql-data-types-and-java-data-types)" section of this topic.

| Method                                                   | Description                                                  |
| -------------------------------------------------------- | ------------------------------------------------------------ |
| void windowUpdate(State state, int, int, int , int, ...) | Updates the data of the window. For more information about UDWFs, see [Window functions](../sql-functions/Window_function.md). Every time when you enter a row as input, this method obtains the window information and updates intermediate results accordingly.<ul><li>`peer_group_start`: the start position of the current partition. `PARTITION BY` is used in the OVER clause to specify a partition column. Rows with the same values in the partition column are considered to be in the same partition.</li><li>`peer_group_end`: the end position of the current partition.</li><li>`frame_start`: the start position of the current window frame. The window frame clause specifies a calculation range, which covers the current row and the rows that are within a specified distance to the current row. For example, `ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING` specifies a calculation range that covers the current row, the previous row before the current row, and the following row after the current row.</li><li>`frame_end`: the end position of the current window frame.</li><li>`inputs`: the data that is entered as the input to a window. The data is an array package that supports only specific data types. In this example, INT values are entered as input, and the array package is Integer[].</li></ul> |

#### Compile a UDTF

A UDTF reads one row of data and returns multiple values that can be considered to be a table. Table-valued functions are typically used to transform rows into columns.

> **NOTE**
>
> StarRocks allows a UDTF to return a table that consists of multiple rows and one column.

Suppose that you want to compile a UDTF named `MY_UDF_SPLIT`. The `MY_UDF_SPLIT` function allows you to use spaces as delimiters and supports request parameters and return parameters of the STRING data type.

```Java
package com.starrocks.udf.sample;

public class UDFSplit{
    public String[] process(String in) {
        if (in == null) return null;
        return in.split(" ");
    }
}
```

The method defined by the user-defined class must meet the following requirements:

> **NOTE**
>
> The data types of the request parameters and return parameters in the method must be the same as those declared in the `CREATE FUNCTION` statement that is to be executed in [Step 6](#step-6-create-the-udf-in-starrocks) and conform to the mapping that is provided in the "[Mapping between SQL data types and Java data types](#mapping-between-sql-data-types-and-java-data-types)" section of this topic.

| Method           | Description                         |
| ---------------- | ----------------------------------- |
| TYPE[] process() | Runs the UDTF and returns an array. |

### Step 4: Package the Java project

Run the following command to package the Java project:

```Bash
mvn package
```

The following JAR files are generated in the **target** folder: **udf-1.0-SNAPSHOT.jar** and **udf-1.0-SNAPSHOT-jar-with-dependencies.jar**.

### Step 5: Upload the Java project

Upload the JAR file **udf-1.0-SNAPSHOT-jar-with-dependencies.jar** to an HTTP server that keeps up and running and is accessible to all FEs and BEs in your StarRocks cluster. Then, run the following command to deploy the file:

```Bash
mvn deploy 
```

You can set up a simple HTTP server by using Python and upload the JAR file to that HTTP server.

> **NOTE**
>
> In [Step 6](#step-6-create-the-udf-in-starrocks), the FEs will check the JAR file that contains the code for the UDF and calculate the checksum, and the BEs will download and execute the JAR file.

### Step 6: Create the UDF in StarRocks

After you upload the JAR file mentioned above, you can create the UDF you have compiled in StarRocks.

#### Syntax

```sql
CREATE [AGGREGATE | TABLE] FUNCTION function_name
(arg_type [, ...])
RETURNS return_type
PROPERTIES ("key" = "value" [, ...])
```

#### Parameters

| **Parameter**      | **Required** | **Description**                                                     |
| ------------- | -------- | ------------------------------------------------------------ |
| AGGREGATE     | No       | Whether to create a UDAF or UDWF.       |
| TABLE         | No       | Whether to create a UDTF. If both `AGGREGATE` and `TABLE` are not specified, a Scalar function is created.               |
| function_name | Yes       | The name of the function you want to create. You can include the name of the database in this parameter, for example,`db1.my_func`. If `function_name` includes the database name, the UDF is created in that database. Otherwise, the UDF is created in the current database. The name of the new function and its parameters cannot be the same as an existing name in the destination database. Otherwise, the function cannot be created. The creation succeeds if the function name is the same but the parameters are different. |
| arg_type      | Yes       | Argument type of the function. The added argument can be represented by `, ...`. For the supported data types, see [Mapping between SQL data types and Java data types](#mapping-between-sql-data-types-and-java-data-types).|
| return_type      | Yes       | The return type of the function. For the supported data types, see [Java UDF](#mapping-between-sql-data-types-and-java-data-types). |
| PROPERTIES    | Yes       | Properties of the function, which vary depending on the type of the UDF to create. |

#### Create a scalar UDF

Run the following command to create the scalar UDF you have compiled in the preceding example:

```SQL
CREATE FUNCTION MY_UDF_JSON_GET(string, string) 
RETURNS string
properties (
    "symbol" = "com.starrocks.udf.sample.UDFJsonGet", 
    "type" = "StarrocksJar",
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

| Parameter | Description                                                  |
| --------- | ------------------------------------------------------------ |
| symbol    | The name of the class for the Maven project to which the UDF belongs. The value of this parameter is in the `<package_name>.<class_name>` format. |
| type      | The type of the UDF. Set the value to `StarrocksJar`, which specifies that the UDF is a Java-based function. |
| file      | The HTTP URL from which you can download the JAR file that contains the code for the UDF. The value of this parameter is in the `http://<http_server_ip>:<http_server_port>/<jar_package_name>` format. |

#### Create a UDAF

Run the following command to create the UDAF you have compiled in the preceding example:

```SQL
CREATE AGGREGATE FUNCTION MY_SUM_INT(INT) 
RETURNS INT
PROPERTIES 
( 
    "symbol" = "com.starrocks.udf.sample.SumInt", 
    "type" = "StarrocksJar",
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

The descriptions of the parameters in PROPERTIES are the same as those in [Create a scalar UDF](#create-a-scalar-udf).

#### Create a UDWF

Run the following command to create the UDWF you have compiled in the preceding example:

```SQL
CREATE AGGREGATE FUNCTION MY_WINDOW_SUM_INT(Int)
RETURNS Int
properties 
(
    "analytic" = "true",
    "symbol" = "com.starrocks.udf.sample.WindowSumInt", 
    "type" = "StarrocksJar", 
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"    
);
```

`analytic`: Whether the UDF is a window function. Set the value to `true`. The descriptions of other properties are the same as those in [Create a scalar UDF](#create-a-scalar-udf).

#### Create a UDTF

Run the following command to create the UDTF you have compiled in the preceding example:

```SQL
CREATE TABLE FUNCTION MY_UDF_SPLIT(string)
RETURNS string
properties 
(
    "symbol" = "com.starrocks.udf.sample.UDFSplit", 
    "type" = "StarrocksJar", 
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

The descriptions of the parameters in PROPERTIES are the same as those in [Create a scalar UDF](#create-a-scalar-udf).

### Step 7: Use the UDF

After you create the UDF, you can test and use it based on your business needs.

#### Use a scalar UDF

Run the following command to use the scalar UDF you have created in the preceding example:

```SQL
SELECT MY_UDF_JSON_GET('{"key":"{\\"in\\":2}"}', '$.key.in');
```

#### Use a UDAF

Run the following command to use the UDAF you have created in the preceding example:

```SQL
SELECT MY_SUM_INT(col1);
```

#### Use a UDWF

Run the following command to use the UDWF you have created in the preceding example:

```SQL
SELECT MY_WINDOW_SUM_INT(intcol) 
            OVER (PARTITION BY intcol2
                  ORDER BY intcol3
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM test_basic;
```

#### Use a UDTF

Run the following command to use the UDTF you have created in the preceding example:

```Plain
-- Suppose that you have a table named t1, and the information about its columns a, b, and c1 is as follows:
SELECT t1.a,t1.b,t1.c1 FROM t1;
> output:
1,2.1,"hello world"
2,2.2,"hello UDTF."

-- Run the MY_UDF_SPLIT() function.
SELECT t1.a,t1.b, MY_UDF_SPLIT FROM t1, MY_UDF_SPLIT(t1.c1); 
> output:
1,2.1,"hello"
1,2.1,"world"
2,2.2,"hello"
2,2.2,"UDTF."
```

> **NOTE**
>
> - The first `MY_UDF_SPLIT` in the preceding code snippet is the alias of the column that is returned by the second `MY_UDF_SPLIT`, which is a function.
> - You cannot use `AS t2(f1)` to specify the aliases of the table and its columns that are to be returned.

## View UDFs

Run the following command to query UDFs:

```SQL
SHOW FUNCTIONS;
```

For more information, see [SHOW FUNCTIONS](../sql-statements/data-definition/SHOW_FUNCTIONS.md).

## Drop UDF

Run the following command to drop a UDF:

```SQL
DROP FUNCTION <function_name>(arg_type [, ...]);
```

For more information, see [DROP FUNCTION](../sql-statements/data-definition/DROP_FUNCTION.md).

## Mapping between SQL data types and Java data types

| SQL TYPE       | Java TYPE         |
| -------------- | ----------------- |
| BOOLEAN        | java.lang.Boolean |
| TINYINT        | java.lang.Byte    |
| SMALLINT       | java.lang.Short   |
| INT            | java.lang.Integer |
| BIGINT         | java.lang.Long    |
| FLOAT          | java.lang.Float   |
| DOUBLE         | java.lang.Double  |
| STRING/VARCHAR | java.lang.String  |

## Parameter settings

Configure the following environment variable in the **be/conf/hadoop_env.sh** file of each Java virtual machine (JVM) in your StarRocks cluster to control memory usage. You can also configure other parameters in the file.

```Bash
export LIBHDFS_OPTS="-Xloggc:$STARROCKS_HOME/log/be.gc.log -server"
```

## FAQ

Can I use static variables when I create UDFs? Do the static variables of different UDFs have mutual impacts on each other?

Yes, you can use static variables when you compile UDFs. The static variables of different UDFs are isolated from each other and do not affect each other even if the UDFs have classes with identical names.
