---
displayed_sidebar: "Chinese"
---

# INSTALL PLUGIN

## 功能

该语句用于安装一个插件。

## 语法

```sql
INSTALL PLUGIN FROM [source] [PROPERTIES ("key"="value", ...)]
```

**source 支持三种类型：**

```plain text
1. 指向一个 zip 文件的绝对路径。
2. 指向一个插件目录的绝对路径。
3. 指向一个 http 或 https 协议的 zip 文件下载路径。
```

**PROPERTIES：**

支持设置插件的一些配置，如设置 zip 文件的 md5sum 的值等。

## 示例

1. 安装一个本地 zip 文件插件：

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo.zip";
    ```

2. 安装一个本地目录中的插件：

    ```sql
    INSTALL PLUGIN FROM "/home/users/starrocks/auditdemo/";
    ```

3. 下载并安装一个插件：

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip";
    ```

4. 下载并安装一个插件，同时设置了 zip 文件的 md5sum 的值：

    ```sql
    INSTALL PLUGIN FROM "http://mywebsite.com/plugin.zip" PROPERTIES("md5sum" = "73877f6029216f4314d712086a146570");
    ```
