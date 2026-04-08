---
displayed_sidebar: docs
---

# StarRocks JDBC 驱动

StarRocks 提供原生 JDBC 驱动，支持任何 JDBC 兼容的客户端、IDE 或应用程序直接连接。

## 前提条件

- Java 8 或更高版本
- 一个正在运行的 StarRocks 集群

## 下载

StarRocks JDBC 驱动可在[Maven Central](https://central.sonatype.com/artifact/com.starrocks/starrocks-connector-j)。

您可以直接从 Maven Central 下载 JAR 包，或按照以下说明将其作为依赖项添加到您的项目中。

## 在您的项目中使用 JAR 包

### Maven

将以下依赖项添加到您的 `pom.xml` 中：

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-connector-j</artifactId>
    <version>1.1.1</version>
</dependency>
```

### Gradle

将以下依赖项添加到您的 `build.gradle` 中：

```groovy
implementation 'com.starrocks:starrocks-connector-j:1.1.1'
```

### 纯 Java

从[Maven Central](https://central.sonatype.com/artifact/com.starrocks/starrocks-connector-j)下载 JAR 包，并在编译和运行时将其添加到 classpath 中：

```bash
javac -cp starrocks-connector-j-<version>.jar MyApp.java
java -cp .:starrocks-connector-j-<version>.jar MyApp
```

## 连接 URL 格式

```
jdbc:starrocks://<fe_host>:<fe_query_port>/<catalog>.<database>
```

| 参数 | 描述 |
|-----------|-------------|
| `fe_host` | StarRocks 集群的 FE 主机 IP 地址。 |
| `fe_query_port` | FE 查询端口，默认为 `9030`。 |
| `catalog` | 要连接的 catalog。内部表使用 `default_catalog`，外部 catalog 使用其名称。 |
| `database` | catalog 中的数据库。 |

**示例：**

```
jdbc:starrocks://192.168.1.1:9030/default_catalog.my_database
```

## 连接属性

| 属性 | 描述 |
|----------|-------------|
| `user` | 登录 StarRocks 的用户名，例如 `admin`。 |
| `password` | 登录 StarRocks 的密码。 |

## 元数据发现

StarRocks JDBC 驱动支持标准的 JDBC 元数据 API (`DatabaseMetaData`)，允许工具内省 catalog、schema、表和列。这使得 IDE 功能（如 schema 浏览、自动补全和表内省）能够开箱即用。

## 示例：从 Java 连接

```java
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class StarRocksExample {
    public static void main(String[] args) throws Exception {
        String url = "jdbc:starrocks://192.168.1.1:9030/default_catalog.my_database";
        Connection conn = DriverManager.getConnection(url, "admin", "password");

        try (Statement stmt = conn.createStatement(); 
             ResultSet rs = stmt.executeQuery("SELECT * FROM my_table LIMIT 10")) {
             while (rs.next()) {
                 System.out.println(rs.getString(1));
             }
        }

        conn.close();
    }
}
```
