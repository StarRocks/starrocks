---
displayed_sidebar: docs
---

# 开发静态扩展

静态扩展是 StarRocks FE 的扩展模块，允许您在不修改核心代码的情况下添加新功能或优化现有功能。与动态插件相比，静态扩展在系统启动时自动加载，并提供更多可注册的扩展点，覆盖系统的核心模块。

此功能从 v4.1 版本开始支持。

## 用法

以下示例演示如何开发静态扩展。

### 前提条件

按如下方式准备 StarRocks FE 开发环境：

```xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-jar-plugin</artifactId>
    <version>3.3.0</version>
    <executions>
        <!-- Core jar -->
        <execution>
            <id>default-jar</id>
            <phase>package</phase>
            <goals>
                <goal>jar</goal>
            </goals>
            <configuration>
                <excludes>
                    <exclude>${your_extension_directory}/**</exclude>
                </excludes>
            </configuration>
        </execution>
        <!-- Extension jar -->
        <execution>
            <id>build-ext-jar</id>
            <phase>package</phase>
            <goals>
                <goal>jar</goal>
            </goals>
            <configuration>
                <finalName>${your_extension_name}</finalName>
                <includes>
                    <include>${your_extension_directory}/**</include>
                </includes>
                <!-- StarRocks will only load the ext.jar -->
                <classifier>ext</classifier>
            </configuration>
        </execution>
    </executions>
</plugin>
```

### 扩展入口点

```java
@SRModule(name = "extension_name")
public class MultiWarehouseExtension implements StarRocksExtension {
    @Override
    public void onLoad(ExtensionContext ctx) {
        // 注册您自己的类或执行其他初始化
        ctx.register(WarehouseManager.class, new MyWarehouseManager());
        ...
    }
}

```

### 日志

构建扩展后，将 `${your_extension_name}-ext.jar` 文件放入 `Config.ext_dir` 目录（默认为 `FE/lib`），然后重启 FE。

FE 启动后的示例日志如下：

```sh
2025-12-26 12:47:46.047+08:00 INFO (main|1) [ExtensionManager.loadExtensionsFromDir():39] start to load extensions
2025-12-26 12:47:46.152+08:00 INFO (main|1) [ExtensionManager.loadExtensions():63] Loaded extension: extension_name
2025-12-26 12:47:46.152+08:00 INFO (main|1) [ExtensionManager.loadExtensionsFromDir():42] all extensions loaded finished
```

- `start to load extensions`：FE 已开始扫描扩展目录。
- `Loaded extension: extension_name`：扩展已成功加载。
- `all extensions loaded finished`：目录中的所有扩展均已加载。
