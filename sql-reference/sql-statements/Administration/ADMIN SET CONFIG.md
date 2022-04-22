# ADMIN SET CONFIG

## 功能

该语句用于设置集群的配置项（当前仅支持设置 FE 的配置项）。
可设置的配置项，可以通过 ADMIN SHOW FRONTEND CONFIG; 命令查看。

设置后的配置项，FE 重启之后会恢复成 fe.conf 中的配置或者默认值，
所以建议设置完之后同时修改一下 fe.conf，防止重启后修改失效。

## 语法

```sql
ADMIN SET FRONTEND CONFIG ("key" = "value");
```

## 示例

1. 设置 'disable_balance' 为 true

    ```sql
    ADMIN SET FRONTEND CONFIG ("disable_balance" = "true");
    ```

## keyword

ADMIN, SET, CONFIG
