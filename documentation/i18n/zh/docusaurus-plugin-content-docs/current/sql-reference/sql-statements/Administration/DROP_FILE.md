# DROP FILE

DROP FILE 语句用于删除保存在数据库中的文件。使用该语句删除一个文件，那么该文件在 FE 内存和 BDBJE 中均会被删除。

## 语法

```SQL
DROP FILE "file_name" [FROM database]

[properties]
```

## 参数说明

| **参数**   | **必填** | **描述**                                         |
| ---------- | -------- | ------------------------------------------------ |
| file_name  | 是       | 文件名。                                           |
| database   | 否       | 文件所属的数据库。                                 |
| properties | 是       | 文件属性，具体配置项见下表：`properties`配置项。     |

`properties` 配置项

| **配置项** | **必填** | **描述**     |
| ---------- | -------- | ------------ |
| catalog    | 是       | 文件所属类别。 |

## 示例

删除文件 `ca.pem`。

```SQL
DROP FILE "ca.pem" properties("catalog" = "kafka");
```
