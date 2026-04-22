---
displayed_sidebar: docs
---

# StarRocks JDBC Driver

StarRocks provides a native JDBC driver that enables direct connectivity from any JDBC-compatible client, IDE, or application.

## Prerequisites

- Java 8 or later
- A running StarRocks cluster

## Download

The StarRocks JDBC driver is available on [Maven Central](https://central.sonatype.com/artifact/com.starrocks/starrocks-connector-j).

You can download the JAR directly from Maven Central, or add it as a dependency in your project using the instructions below.

### Download via Maven CLI

If you have Maven installed, you can download the JAR without creating a project:

```bash
mvn dependency:get -Dartifact=com.starrocks:starrocks-connector-j:1.1.1
```

The JAR will be saved to your local Maven repository at:

```
~/.m2/repository/com/starrocks/starrocks-connector-j/1.1.1/starrocks-connector-j-1.1.1.jar
```

## Use the JAR in your project

### Maven

Add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>com.starrocks</groupId>
    <artifactId>starrocks-connector-j</artifactId>
    <version>1.1.1</version>
</dependency>
```

### Gradle

Add the following dependency to your `build.gradle`:

```groovy
implementation 'com.starrocks:starrocks-connector-j:1.1.1'
```

### Plain Java

Download the JAR from [Maven Central](https://central.sonatype.com/artifact/com.starrocks/starrocks-connector-j) and add it to the classpath when compiling and running:

```bash
javac -cp starrocks-connector-j-<version>.jar MyApp.java
java -cp .:starrocks-connector-j-<version>.jar MyApp
```

## Connection URL format

```
jdbc:starrocks://<fe_host>:<fe_query_port>/<catalog>.<database>
```

| Parameter | Description |
|-----------|-------------|
| `fe_host` | The FE host IP address of your StarRocks cluster. |
| `fe_query_port` | The FE query port, default `9030`. |
| `catalog` | The catalog to connect to. Use `default_catalog` for internal tables, or the name of an external catalog. |
| `database` | The database within the catalog. |

**Example:**

```
jdbc:starrocks://192.168.1.1:9030/default_catalog.my_database
```

## Connection properties

| Property | Description |
|----------|-------------|
| `user` | The username to log in to StarRocks, for example, `admin`. |
| `password` | The password to log in to StarRocks. |

## Metadata discovery

The StarRocks JDBC driver supports standard JDBC metadata APIs (`DatabaseMetaData`), which allow tools to introspect catalogs, schemas, tables, and columns. This enables IDE features such as schema browsing, auto-complete, and table introspection to work out of the box.

## Example: connect from Java

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
