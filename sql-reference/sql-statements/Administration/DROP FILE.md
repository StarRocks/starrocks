# DROP FILE

## 功能

该语句用于删除一个已上传的文件。

## 语法

```sql
DROP FILE "file_name" [FROM database]
[properties];
```

注：方括号 [] 中内容可省略不写。

说明：

```plain text
file_name: 文件名。
database:  文件归属的某一个 db，如果没有指定，则使用当前 session 的 db。

properties 支持以下参数:
    catalog: 必须。文件所属分类。
```

## 示例

1. 删除文件 ca.pem

    ```sql
    DROP FILE "ca.pem" properties("catalog" = "kafka");
    ```

## 关键字(keywords)

DROP，FILE
