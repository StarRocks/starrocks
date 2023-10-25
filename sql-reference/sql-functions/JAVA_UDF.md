# Java UDF【公测中】

自 2.2.0 版本起，StarRocks 支持使用 Java 语言编写用户定义函数 UDF（User Defined Function，UDF），该功能正在公测中。您可以根据业务场景开发自定义函数，扩展StarRocks的函数能力。本文介绍StarRocks支持的 UDF 类型，开发流程和使用方式。

## UDF 类型

目前StarRocks支持的 UDF 包括 Scalar UDF（用户自定义标量函数）、UDAF（用户自定义聚合函数）、UDWF（用户自定义窗口函数）、UDTF（用户自定义表格函数）。

- Scalar UDF：用户自定义标量函数，对单行数据进行操作，输出单行结果。当您在查询时使用 Scalar UDF ，每行数据最终都会按行出现在结果集中。典型的标量函数包括 UPPER、LOWER、ROUND、ABS。
- UDAF：用户自定义的聚合函数，对多行数据进行操作，输出单行结果。典型的聚合函数包括 SUM、COUNT、MAX、MIN，这些函数对于每个GROUP BY分组中多行数据进行聚合后，只输出一行结果。
- UDWF：用户自定义窗口函数。跟普通聚合函数不同的是，窗口函数针对一组行（一个窗口）计算值，并为每行返回一个结果。一般情况下，窗口函数包含 OVER 子句，将数据行拆分成多个分组，窗口函数基于每一行数据所在的组（一个窗口）进行计算，并为每行返回一个结果。
- UDTF：用户自定义表值函数，读入一行数据，输出多个值可视为一张表。表值函数常用于实现行转列。

## 开启 UDF

FE 配置项：在 FE 配置文件（$FE_HOME/conf/fe.conf）中设置配置项 `enable_udf = true`，并且需要重启FE才能使之生效。

## 开发并使用UDF

### Scalar UDF

#### 步骤一：创建 Maven 项目

1. 创建 Maven 项目，项目的基本目录结构如下：

    ```Plain Text
    project
    |--pom.xml
    |--src
    |  |--main
    |  |  |--java
    |  |  |--resources
    |  |--test
    |--target
    ```

2. 在pom.xml中添加如下依赖：

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

#### 步骤二：开发 Scalar UDF 函数

1. 编写 Scalar UDF 代码，示例如下。

   我们以业务场景中“提取 JSON 数据”为例进行说明。例如，业务场景中，JSON 数据中某个字段的值可能是 JSON  字符串而不是 JSON 对象，因此在提取 JSON  字符串时，SQL 语句需要嵌套调用`GET_JSON_STRING`，例如：`GET_JSON_STRING(GET_JSON_STRING('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key"), "$.k0")`。

   为简化SQL语句，您可以开发一个 UDF，直接提取 JSON  字符串，例如：`MY_UDF_JSON_GET('{"key":"{\\"k0\\":\\"v0\\"}"}', "$.key.k0")`。

    ```Java
    package com.starrocks.udf.sample;

    import com.alibaba.fastjson.JSONPath;

    public class UDFJsonGet {
        public final String evaluate(String obj, String key) {
            if (obj == null || key == null) return null;
            try {
                // JSONPath 库可以全部展开，即使某个字段的值是 JSON 格式的字符串
                return JSONPath.read(obj, key).toString();
            } catch (Exception e) {
                return null;
            }
        }
    }
    ```

    用户自定义类必须实现如下方法：

    > 方法中请求参数和返回参数的数据类型，需要和步骤三中的 `CREATE FUNCTION` 语句中声明的相同，且两者的类型映射关系需要符合[类型映射关系](./JAVA_UDF.md/#类型映射关系)。

    | **方法**                 | **含义**                                               |
    | ------------------------ | ------------------------------------------------------ |
    | TYPE evaluate(TYPE, ...) | evaluate 方法为 UDF 调用入口，必须是 public 成员方法。 |

2. 执行`mvn package`，进行打包。

   target 目录下会生成两个文件： `udf-1.0-SNAPSHOT.jar`、  `udf-1.0-SNAPSHOT-jar-with-dependencies.jar`。

3. 将文件`udf-1.0-SNAPSHOT-jar-with-dependencies.jar`上传至 FE 和 BE 能访问的 HTTP 服务器，并且HTTP服务需要一直开启。

    > 步骤三中， FE 会对 UDF 所在 Jar 包进行校验并计算校验值，BE 会下载 UDF 所在 Jar 包并执行。

#### 步骤三：在 StarRocks 中创建 Scalar UDF 函数

执行如下命令，在 StarRocks 中创建 Scalar UDF 函数。

```SQL
CREATE FUNCTION MY_UDF_JSON_GET(string, string) 
RETURNS string
properties (
    "symbol" = "com.starrocks.udf.sample.UDFJsonGet", 
    "type" = "StarrocksJar",
    "file" = "http://http_host:http_port/udf-1.0-SNAPSHOT-jar-with-dependencies.jar"
);
```

参数说明：

- **symbol**：UDF 所在项目的类名。格式为`<包名>.<类名>`。
- **type**：用于标记所创建的 UDF 类型。取值为 **StarrocksJar**，表示基于 Java 的 UDF。
- **file**：UDF 所在 Jar 包的 HTTP 路径。格式为`http://<``HTTP服务器``的IP地址>:<``HTTP服务器的``端口号>/Jar包的名称`。

#### 步骤四：使用 Scalar UDF 函数

执行如下命令，使用Scalar UDF函数。

```SQL
SELECT MY_UDF_JSON_GET('{"key":"{\\"in\\":2}"}', '$.key.in');
```

### UDAF

#### 步骤一：创建 Maven 项目

请参考[步骤一：创建 Maven 项目](./JAVA_UDF.md/#步骤一创建-maven-项目)。

#### 步骤二：开发 UDAF 函数

1. 编写  UDAF 的代码，示例如下。

   本文以 `MY_SUM_INT`  函数为例进行说明。与内置函数`SUM`（返回值为 BIGINT 类型）区别在于，`MY_SUM_INT`  函数支持传入参数和返回参数的类型为 INT。

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

    用户自定义类必须实现如下方法：

    > 方法中传入参数和返回参数的数据类型，需要和步骤三中的 `CREATE FUNCTION` 语句中声明的相同，且两者的类型映射关系需要符合[类型映射关系](./JAVA_UDF.md/#类型映射关系)。

    | **需要实现的方法**                | **说明**                                                     |
    | --------------------------------- | ------------------------------------------------------------ |
    | State create()                    | 创建 State。                                                 |
    | void destroy(State)               | 销毁 State。                                                 |
    | void update(State, ...)           | 更新 State 。其中第一个参数是State，其余的参数是函数声明的入参，可以为 1 个或多个。 |
    | void serialize(State, ByteBuffer) | 序列化 State。                                               |
    | void merge(State, ByteBuffer)     | 合并 State 和反序列化 State。                                |
    | TYPE finalize(State)              | 通过 State 获取函数的最终结果。                              |

    并且，开发 UDAF 函数时，需要使用缓冲区类 `java.nio.ByteBuffer` 和局部变量 `serializeLength`，用于保存和表示中间结果，指定中间结果的序列化长度。

    | **类和局部变量**      | **说明**                                                     |
    | --------------------- | ------------------------------------------------------------ |
    | java.nio.ByteBuffer() | 缓冲区类，用于保存中间结果。 并且，由于中间结果在不同执行节点间传输时，会进行序列化和反序列化，因此还需要使用 serializeLength 指定中间结果序列化后的长度。 |
    | serializeLength()     | 中间结果序列化后的长度，单位为Byte。 serializeLength的数据类型固定为INT。 例如，示例中`State { int counter = 0; public int serializeLength() { return 4; }}`包含对中间结果序列化后的说明，即，中间结果的数据类型为INT，序列化长度为4 Byte。您也可以按照业务需求进行调整，例如中间结果序列化后的数据类型LONG，序列化长度为8 Byte，则需要传入`State { long counter = 0; public int serializeLength() { return 8; }}`。 |

   `java.nio.ByteBuffer` 序列化的注意事项：

    > - 不支持依赖 ByteBuffer 的 remaining 方法来反序列化 State。
    > - 不支持对 ByteBuffer 调用 clear 方法。
    > - serializeLength 需要与实际写入数据的长度保持一致，否则序列化和反序列化过程中会造成结果错误。

2. 执行`mvn package`，进行打包。

   target 目录下会生成两个文件： `udf-1.0-SNAPSHOT.jar`、  `udf-1.0-SNAPSHOT-jar-with-dependencies.jar`。

3. 将文件`udf-1.0-SNAPSHOT-jar-with-dependencies.jar`上传至 FE 和 BE 能访问的 HTTP 服务器，并且HTTP服务需要一直开启。

    > 步骤三中， FE 会对 UDF 所在 Jar 包进行校验并计算校验值，BE 会下载 UDF 所在 Jar 包并执行。

#### 步骤三：在 StarRocks 中创建 UDAF 函数

执行如下命令，在StarRocks中创建 UDAF 函数。

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

> 参数说明与 Scalar UDF 函数类似，请参考[在 StarRocks 中创建 Scalar UDF 函数](./JAVA_UDF.md/#步骤三在-starrocks-中创建-scalar-udf-函数)。

#### 步骤四：使用 UDAF 函数

执行如下命令，使用 UDAF 函数。

```SQL
SELECT MY_SUM_INT(col1);
```

### UDWF

#### 步骤一：创建 Maven 项目

请参考[步骤一：创建 Maven 项目](./JAVA_UDF.md/#步骤一创建-maven-项目)。

#### 步骤二：开发 UDWF 函数

1. 编写  UDWF 的代码，示例如下。

   本文以 `MY_WINDOW_SUM_INT`  函数为例进行说明。与内置函数`SUM`（返回类型为 BIGINT）区别在于，`MY_WINDOW_SUM_INT`  函数支持传入参数和返回参数的类型为 INT。

    ```Java
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

    用户自定义类必须实现 UDAF 所需要的方法（窗口函数是特殊聚合函数)，以及方法 `windowUpdate()`。

    > 方法中请求参数和返回参数的数据类型，需要和步骤三中的 `CREATE FUNCTION` 语句中声明的相同，且两者的类型映射关系需要符合[类型映射关系](./JAVA_UDF.md/#类型映射关系)。

    <table>
       <tr>
          <td>需要额外实现的方法</td>
          <td>方法的含义</td>
       </tr>
       <tr>
          <td>void windowUpdate(State state, int, int, int , int, ...)</td>
          <td>更新窗口数据。窗口函数的详细说明，请参见[窗口函数](./Window_function.md)。输入每一行数据，都会获取到对应窗口信息来更新中间        结果。
        <ul>
          <li>peer_group_start：是当前分区开始的位置。<br />分区：OVER子句中 PARTITION BY 指定分区列， 分区列的值相同的行被视为在同一个分区内。</li>
          <li>peer_group_end：当前分区结束的位置。</li>
          <li>frame_start：当前窗口框架（window frame）起始位置。<br />窗口框架：window frame子句指定了运算范围，以当前行为准，前后若干行作为窗口函数运算的对象。例如ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING，表示运算范围为当前行和它前后各一行数据。</li>
          <li>frame_end：当前窗口框架（window frame）结束位置。</li>
          <li>inputs：表示一个窗口中输入的数据，为包装类数组。包装类需要对应输入数据的类型，本示例中输入数据类型为INT，因此包装类数组为Integer[]。</li>
        </ul></td>
       </tr>
    </table>

2. 执行`mvn package`，进行打包。

   target 目录下会生成两个文件： `udf-1.0-SNAPSHOT.jar`、  `udf-1.0-SNAPSHOT-jar-with-dependencies.jar`。

3. 将文件`udf-1.0-SNAPSHOT-jar-with-dependencies.jar`上传至 FE 和 BE 能访问的 HTTP 服务器，并且HTTP服务需要一直开启。

    > 步骤三中， FE 会对 UDF 所在 Jar 包进行校验并计算校验值，BE 会下载 UDF 所在 Jar 包并执行。

#### 步骤三：在 StarRocks 中创建 UDWF 函数

执行如下命令，在StarRocks中创建 UDWF 函数。

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

> - 参数Analytic：固定取值为**true**，表示所创建的函数为窗口函数。
> - 其他参数的说明与 Scalar UDF 函数类似，请参考[在 StarRocks 中创建 Scalar UDF 函数](./JAVA_UDF.md/#步骤三在-starrocks-中创建-scalar-udf-函数)。

#### 步骤四：使用 UDWF 函数

执行如下命令，使用UDWF函数。

```SQL
SELECT MY_WINDOW_SUM_INT(intcol) 
            OVER (PARTITION BY intcol2
                  ORDER BY intcol3
                  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM test_basic;
```

### UDTF

> 说明：目前 UDTF 只支持返回多行单列。

#### 步骤一：创建 Maven 项目

请参考[步骤一：创建 Maven 项目](./JAVA_UDF.md/#步骤一创建-maven-项目)。

#### 步骤二：开发 UDTF 函数

1. 编写  UDTF 的代码，示例如下。

    本文以 `MY_UDF_SPLIT` 函数为例进行说明。`MY_UDF_SPLIT` 函数支持分隔符为`空格`，传入参数和返回参数的类型为 STRING。

    ```TypeScript
    public class UDFSplit{
        public String[] process(String in) {
            if (in == null) return null;
            return in.split(" ");
        }
    }
    ```

    用户自定义类必须实现如下方法：

    > 方法中请求参数和返回参数的数据类型，需要和步骤三中的 `CREATE FUNCTION` 语句中声明的相同，且两者的类型映射关系需要符合[类型映射关系](./JAVA_UDF.md/#类型映射关系)。

    | **需要实现的方法** | **方法的含义**                              |
    | ------------------ | ------------------------------------------- |
    | TYPE[] process()   | process 方法为 UDTF 调用入口，需要返回数组。 |

2. 执行`mvn package`，进行打包。

   target 目录下会生成两个文件： `udf-1.0-SNAPSHOT.jar`、  `udf-1.0-SNAPSHOT-jar-with-dependencies.jar`。

3. 将文件`udf-1.0-SNAPSHOT-jar-with-dependencies.jar`上传至 FE 和 BE 能访问的 HTTP 服务器，并且HTTP服务需要一直开启。

    > 步骤三中， FE 会对 UDF 所在 Jar 包进行校验并计算校验值，BE 会下载 UDF 所在 Jar 包并执行。

#### 步骤三：在 StarRocks 中创建 UDTF 函数

执行如下命令，在 StarRocks 中创建 UDTF 函数。

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

> 参数说明与 Scalar UDF 函数类似，请参考[在 StarRocks 中创建 Scalar UDF 函数](./JAVA_UDF.md/#步骤三在-starrocks-中创建-scalar-udf-函数)。

#### 步骤四：使用 UDTF 函数

执行如下命令，使用 UDTF 函数。

```Plain Text
-- 假设存在表t1，其列a、b、c1信息如下
SELECT t1.a,t1.b,t1.c1 FROM t1;
> output:
1,2.1,"hello world"
2,2.2,"hello UDTF."

-- 使用 MY_UDF_SPLIT 函数
SELECT t1.a,t1.b, MY_UDF_SPLIT FROM t1, MY_UDF_SPLIT(t1.c1); 
> output:
1,2.1,"hello"
1,2.1,"world"
2,2.2,"hello"
2,2.2,"UDTF."
```

> - 第一个 MY_UDF_SPLIT 为调用`MY_UDF_SPLIT`后生成的列别名。
> - 暂不支持使用`AS t2(f1)`的方式指定表格函数返回表的表别名和列别名。

## 管理 UDF

执行`SHOW FUNCTIONS`，查看函数信息。更多信息，请参见[SHOW FUNCTIONS](../sql-statements/data-definition/show-functions.md)。

## 删除 UDF

运行以下命令删除指定的 UDF。

```sql
DROP FUNCTION function_name(arg_type [, ...]);
```

更多信息，请参见[DROP FUNCTION](../sql-statements/data-definition/drop-function.md)。

## 类型映射关系

| **SQL TYPE**   | **Java TYPE**     |
| -------------- | ----------------- |
| BOOLEAN        | java.lang.Boolean |
| TINYINT        | java.lang.Byte    |
| SMALLINT       | java.lang.Short   |
| INT            | java.lang.Integer |
| BIGINT         | java.lang.Long    |
| FLOAT          | java.lang.Float   |
| DOUBLE         | java.lang.Double  |
| STRING/VARCHAR | java.lang.String  |

## 参数配置

JVM 参数配置：在 `be/conf/hadoop_env.sh` 中配置如下环境变量，可以控制内存使用。

```Bash
export LIBHDFS_OPTS="-Xloggc:$STARROCKS_HOME/log/be.gc.log -server"
```

## FAQ

Q：开发 UDF 时是否可以使用静态变量？不同 UDF 间的静态变量间否会互相影响？<br />
A：支持在开发 UDF 时使用静态变量，且不同 UDF 间（即使类同名），静态变量是互相隔离的，不会互相影响。
