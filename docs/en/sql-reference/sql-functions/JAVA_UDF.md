# Java UDFs

StarRocks has started to support user-defined functions (UDFs) since v2.2.0. You can create UDFs based on your business requirements to extend the capabilities of StarRocks. This topic describes the UDF types that are supported by StarRocks and how to create and use UDFs in StarRocks.

## UDF types

StarRocks supports scalar UDFs, user-defined aggregate functions (UDAFs), user-defined window functions (UDWFs), and user-defined table functions (UDTFs).

- Scalar UDFs: A scalar UDF takes a single row and returns a single row as a result. When you use a scalar UDF as part of your query, each returned row is included as a single row in the result set. Typical scalar UDFs include UPPER, LOWER, ROUND, and ABS.

- UDAFs: A UDAF takes multiple rows and returns a single row as a result. Typical UDAFs include SUM, COUNT, MAX, and MIN. These UDAFs aggregate multiple rows in each GROUP BY clause and return one aggregated row.

- UDWFs: A UDWF takes a group of rows and returns one result for each row. In this sense, the group of rows is known as a window. In normal cases, a UDWF incorporates an OVER clause. The OVER clause divides rows into groups. The UDWF performs calculations on each group of rows and returns one result for each row.

- UDTFs: A UDTF takes one row and returns a visualized table that consists of multiple rows. UDTFs are generally used to convert rows to columns.

## Enable UDFs

In the **$FE_HOME/conf/fe.conf** file of each frontend (FE), set `enable_udf` to `true`. Then, restart that FE to make the new setting take effect.

## Create and use UDFs

### Create and use a scalar UDF

#### Step 1: Create a Maven project

1. Create a Maven project, whose basic directory structure is as follows:

    ```Plain_Text
    project
    |--pom.xml
    |--src
    |  |--main
    |  |  |--java
    |  |  |--resources
    |  |--test
    |--target
    ```

2. Add the following dependencies to the **pom.xml** file:

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

#### Step 2: Compile code for a scalar UDF

1. Compile code for the scalar UDF that you want to create.

    For example, the value of a field in a JSON document is a JSON string rather than a JSON object. When you execute an SQL statement to extract the JSON string, you must run the GET_JSON_STRING function twice in the SQL statement. Example:

    ```SQL
    GET_JSON_STRING(GET_JSON_STRING('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key"), "$.k0")
    ```

    To simplify the SQL statement, you can create a scalar UDF that can directly extracts a JSON string. Example:

    ```SQL
    MY_UDF_JSON_GET('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key.k0")
    package com.starrocks.udf.sample;



    import com.alibaba.fastjson.JSONPath;



    public class UDFJsonGet {

        public final String evaluate(String obj, String key) {

            if (obj == null || key == null) return null;

            try {

                // The JSONPath library can be completely expanded even if the values of a specific field are JSON strings.

                return JSONPath.read(obj, key).toString();

            } catch (Exception e) {

                return null;

            }

        }

    }
    ```

    The class that you define must implement the method that is described in the following table.

    > Note: The input and return data types in the method must be the same as the data types declared in the CREATE FUNCTION statement that is to be executed in Step 3. Additionally, the input and return data types in the method and the declared data types must conform to the mapping that is provided in the "Mapping between SQL data types and Java data types" section of this topic.

    | od                         | Description                                                   |
    | -------------------------- | ------------------------------------------------------------ |
    | TYPE1 evaluate(TYPE2, ...) | The evaluate method requires the public member access level. |

2. Run `mvn package` to package the code for the scalar UDF.

    The following two JAR files are generated in the **target** folder: **udf-1.0-SNAPSHOT.jar** and **udf-1.0-SNAPSHOT-jar-with-dependencies.jar**.

3. Upload the **udf-1.0-SNAPSHOT-jar-with-dependencies.jar** file to an HTTP server that is accessible to all FEs and backends (BEs) in your cluster. Make sure that the HTTP service remains enabled.

    > Note: In Step 3, the FEs check the JAR files and calculate the checksum, and the BEs download and execute the JAR files.

#### Step 3: Create a scalar UDF in StarRocks

Execute the following statement to create a scalar UDF in StarRocks:

```SQL
CREATE FUNCTION MY_UDF_JSON_GET(string, string) 

RETURNS string

properties (

    "symbol" = "com.starrocks.udf.sample.UDFJsonGet", 

    "type" = "StarrocksJar",

    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"

);
```

Parameter description:

- **symbol**: the name of the class for the Maven project to which the scalar UDF belongs. The value of this parameter is in the `<Package name>.<Class name>` format.

- **type**: the type of the scalar UDF. Set the value to **StarrocksJar**, which specifies that the scalar UDF is a Java-based function.

- **file**: the HTTP URL from which you can download the JAR file that contains the code for the scalar UDF.

#### Step 4: Use the scalar UDF that you created

Execute the following statement to run the scalar UDF that you created:

```SQL
SELECT MY_UDF_JSON_GET('{"key":"{\\"in\\":2}"}', '$.key.in');
```

### Create and use a UDAF

#### Step 1: Create a Maven project

For more information, see "Step 1: Create a Maven project" in the "Create and use a scalar UDF" section of this topic.

#### Step 2: Compile code for a UDAF

1. Compile code for the UDAF that you want to create.

    Suppose that you want to create a UDAF named MY_SUM_INT. Unlike the built-in aggregate function SUM, which returns BIGINT values, the SUMINT aggregate function supports only input and return parameters of the INT data type.

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

    The class that you define must implement the methods that are described in the following table.

    > Note: The input and return data types in the methods must be the same as the data types declared in the CREATE FUNCTION statement that is to be executed in Step 3. Additionally, the input and return data types in the methods and the declared data types must conform to the mapping that is provided in the "Mapping between SQL data types and Java data types" section of this topic.

    | Method                            | Description                                                   |
    | --------------------------------- | ------------------------------------------------------------ |
    | State create()                    | Creates a state.                                             |
    | void destroy(State)               | Destroys a state.                                            |
    | void update(State, ...)           | Updates a state. In addition to the first parameter State, you can also specify one or more input parameters in the function declaration. |
    | void serialize(State, ByteBuffer) | Serializes a state into the byte buffer.                     |
    | void merge(State, ByteBuffer)     | Deserializes a state from the byte buffer, and merges the byte buffer into the state as the first parameter. |
    | TYPE finalize(State)              | Obtains the final result of the function from a state.       |

    During the coding process, you must also use the `java.nio.ByteBuffer` buffer class, which stores intermediate results, and the `serializeLength` local variable, which specifies the length that is allowed for the deserialization of intermediate results.

    | nd local variable     | Description                                                  |
    | --------------------- | ------------------------------------------------------------ |
    | java.nio.ByteBuffer() | The buffer class, which stores intermediate results. Intermediate results may be serialized or deserialized when they are transmitted between nodes for execution. Therefore, you must also use the serializeLength variable to specify the length that is allowed for the deserialization of intermediate results. |
    | serializeLength()     | The length that is allowed for the deserialization of intermediate results. Unit: bytes. This parameter must be set to an INT value. For example, `State { int counter = 0; public int serializeLength() { return 4; }}` specifies that intermediate results are of the INT data type and the length for deserialization is 4 bytes. You can adjust these settings based on your business requirements. For example, if you want to specify the data type of intermediate results as LONG and the length for deserialization as 8 bytes, pass `State { long counter = 0; public int serializeLength() { return 8; }}`. |

    Take note of the following points for the deserialization of intermediate results stored in the `java.nio.ByteBuffer` class:

    - > The Remaining method that is dependent on the ByteBuffer class cannot be called to deserialize a state.

    - > The Clear method cannot be called on the ByteBuffer class.

    - > The value of the serializeLength variable must be the same as the length of the written-in data. Otherwise, incorrect results are generated during serialization and deserialization.

2. Run `mvn package` to package the code.

    The following two JAR files are generated in the **target** folder: **udf-1.0-SNAPSHOT.jar** and **udf-1.0-SNAPSHOT-jar-with-dependencies.jar**.

3. Upload the **udf-1.0-SNAPSHOT-jar-with-dependencies.jar** file to an HTTP server that is accessible to all FEs and BEs in your cluster. Make sure that the HTTP service remains enabled.

    > Note: In Step 3, the FEs check the JAR files and calculate the checksum, and the BEs download and execute the JAR files.

#### Step 3: Create a UDAF in StarRocks

Execute the following statement to create a UDAF in StarRocks:

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

> The parameters for a UDAF are similar to those for a scalar UDF. For more information about the parameters, see the "Create and use a scalar UDF" section of this topic.

#### Step 4: Use the UDAF that you created

Execute the following statement to run the UDAF that you created:

```SQL
SELECT MY_SUM_INT(col1) from t1;
```

### Create and use a UDWF

#### Step 1: Create a Maven project

For more information, see "Step 1: Create a Maven project" in the "Create and use a scalar UDF" section of this topic.

#### Step 2: Compile code for a UDWF

1. Compile code for the UDWF that you want to create.

    Suppose that you want to create a UDWF named MY_WINDOW_SUM_INT. Unlike the SUM window function, which returns BIGINT values, the MY_WINDOW_SUM_INT window function supports only input and return parameters of the INT data type.

    ```Java
    public class WindowSumInt {    

        public static class State {

            int counter = 0;

            public int serializeLength() { return 4; }

            @Override

            public String toString() {

                return "State{counter=" + counter + "}";

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

    The class that you define must implement the method that UDAF requires and the `windowUpdate()` method.

    > The input and return data types in the method must be the same as the data types declared in the CREATE FUNCTION statement that is to be executed in Step 3. Additionally, the input and return data types and the declared data types must conform to the mapping that is provided in the "Mapping between SQL data types and Java data types" section of this topic.

    | Method                                                   | Description                                                  |
    | -------------------------------------------------------- | ------------------------------------------------------------ |
    | void windowUpdate(State state, int, int, int , int, ...) | Updates the data of a window. For more information about UDWFs, see [Window functions](./Window_function.md). Every time when you enter a row as input, this method obtains the window information and updates intermediate results accordingly. - peer_group_start: the start position of the current partition. PARTITION BY is used in the OVER clause to specify a partition column. Rows with the same values in the partition column are considered to be in the same partition. - peer_group_end: the end position of the current partition. - frame_start: the start position of the current window frame. The window frame clause specifies a calculation range, which covers the current row and the rows that are within a specified distance to the current row. For example, ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING specifies a calculation range that covers the current row, the previous row before the current row, and the following row after the current row. - frame_end: the end position of the current window frame. - inputs: the data that is entered as the input to a window. The data is an array package that supports only specific data types. In this example, INT values are entered as input, and the array package is Integer[]. |

2. Run `mvn package` to package the code.

    The following two JAR files are generated in the **target** folder: **udf-1.0-SNAPSHOT.jar** and **udf-1.0-SNAPSHOT-jar-with-dependencies.jar**.

3. Upload the **udf-1.0-SNAPSHOT-jar-with-dependencies.jar** file to an HTTP server that is accessible to all FEs and BEs in your cluster. Make sure that the HTTP service remains enabled.

    > Note: In Step 3, the FEs check the JAR files and calculate the checksum, and the BEs download and execute the JAR files.

#### Step 3: Create a UDWF in StarRocks

Execute the following statement to create a UDWF in StarRocks:

```SQL
CREATE AGGREGATE FUNCTION MY_WINDOW_SUM_INT(Int)

RETURNS Int

properties 

(

    "analytic" = "true",

    "symbol" = "WindowSumInt", 

    "type" = "StarrocksJar", 

    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"    

);
```

> Note:

- > The value of the `analytic` parameter is fixed as `true`, which specifies that the UDWF is a window function (not a aggregate function).

- > The other parameters for a UDWF are similar to those for a scalar UDF. For more information, see [Create a scalar UDF in StarRocks](#step-3-create-a-scalar-udf-in-starrocks).

#### Step 4: Use the UDWF that you created

Execute the following statement to run the UDWF that you created:

```SQL
SELECT MY_WINDOW_SUM_INT(intcol) 

            OVER (PARTITION BY intcol2

                  ORDER BY intcol3

                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

FROM test_basic;
```

### Create and use a UDTF

> StarRocks allows a UDTF to return a table that consists of multiple rows and one column.

#### Step 1: Create a Maven project

For more information, see "Step 1: Create a Maven project" in the "Create and use a scalar UDF" section of this topic.

#### Step 2: Compile code for a UDTF

1. Compile code for the UDTF that you want to create.

    Suppose that you want to create a UDTF named MY_UDF_SPLIT. The MY_UDF_SPLIT function allows you to use spaces as delimiters and supports input and return parameters of the STRING data type.

    ```TypeScript
    public class UDFSplit{

        public String[] process(String in) {

            if (in == null) return null;

            return in.split(" ");

        }

    }
    ```

    The class that you define must implement the method that is described in the following table.

    > The input and return data types in the method must be the same as the data types declared in the CREATE FUNCTION statement that is to be executed in Step 3. Additionally, the input and return data types in the method and the declared data types must conform to the mapping that is provided in the "Mapping between SQL data types and Java data types" section of this topic.

    | Method           | Descrition                        |
    | ---------------- | --------------------------------- |
    | TYPE[] process() | Runs a UDTF and returns an array. |

2. Run `mvn package` to package the code.

    The following two JAR files are generated in the **target** folder: **udf-1.0-SNAPSHOT.jar** and **udf-1.0-SNAPSHOT-jar-with-dependencies.jar**.

3. Upload the **udf-1.0-SNAPSHOT-jar-with-dependencies.jar** file to an HTTP server that is accessible to all FEs and BEs in your cluster. Make sure that the HTTP service remains enabled.

    > Note: In Step 3, the FEs check the JAR files and calculate the checksum, and the BEs download and execute the JAR files.

#### Step 3: Create a UDTF in StarRocks

Execute the following statement to create a UDTF in StarRocks:

```SQL
CREATE TABLE FUNCTION MY_UDF_SPLIT(string)

RETURNS string

properties 

(

    "symbol" = "UDFSplit", 

    "type" = "StarrocksJar", 

    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"

);
```

> Note: The parameters for a UDTF are similar to those for a scalar UDF. For more information, see the "Create and use a scalar UDF" section of this topic.

#### Step 4: Use the UDTF that you created

Execute the following statements to run the UDTF that you created:

```SQL
-- Suppose that you have a table named t1, and the information

-- about its columns a, b, and c1 is as follows:

SELECT t1.a,t1.b,t1.c1 FROM t1;

> output:

1,2.1,"hello world"

2,2.2,"hello UDTF."



-- Run the MY_UDF_SPLIT function.

SELECT t1.a,t1.b, MY_UDF_SPLIT FROM t1, MY_UDF_SPLIT(t1.c1); 

> output:

1,2.1,"hello"

1,2.1,"world"

2,2.2,"hello"

2,2.2,"UDTF."
```

- > The first `MY_UDF_SPLIT` in the preceding statement is the alias of the column that is returned by the second `MY_UDF_SPLIT`, which is a function.

- > You cannot use `AS t2(f1)` to specify the aliases of the table and its columns that are to be returned recently.

## Manage UDFs

Execute the SHOW FUNCTIONS statement to query UDFs. For more information, see [SHOW FUNCTIONS](../sql-statements/data-definition/show-functions.md).

## Delete UDFs

Execute the DROP FUNCTION statement to delete a UDF. For more information, see [DROP FUNCTION](../sql-statements/data-definition/drop-function.md).

## Mapping between SQL data types and Java data types

| SQL data type      | Java data type    |
| ------------------ | ----------------- |
| BOOLEAN            | java.lang.Boolean |
| TINYINT            | java.lang.Byte    |
| SMALLINT           | java.lang.Short   |
| INT                | java.lang.Integer |
| BIGINT             | java.lang.Long    |
| FLOAT              | java.lang.Float   |
| DOUBLE             | java.lang.Double  |
| STRING and VARCHAR | java.lang.String  |

## Parameter settings

Configure the following environment variable in the **be/conf/hadoop_env.sh** file of each Java virtual machine (JVM) in your cluster to control the usage of memory resources:

```Bash
export LIBHDFS_OPTS="-Xloggc:$STARROCKS_HOME/log/be.gc.log -server"
```

## FAQ

Can I use static variables when I create UDFs? Do the static variables of different UDFs have mutual impacts on each other?

Yes, you can use static variables when you compile UDFs. The static variables of different UDFs are isolated from each other and do not affect each other even if the UDFs have classes with identical names.
