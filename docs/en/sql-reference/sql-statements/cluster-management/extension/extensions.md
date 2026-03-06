---
displayed_sidebar: docs
---

Static Extensions are StarRocks FE’s extension modules that allow you to add new features or optimize existing functionality without modifying the core code. Compared to dynamic plugins, static extensions are automatically loaded at system startup and provide more registrable extension points, covering the system’s core modules.

## Examples

This example demonstrates how to develop a static extension.

**Prerequisites:**

StarRocks FE development environment.

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

**Extension entry point:**

```java
@SRModule(name = "extension_name")
public class MultiWarehouseExtension implements StarRocksExtension {
    @Override
    public void onLoad(ExtensionContext ctx) {
        // Register your own class or perform other initialization
        ctx.register(WarehouseManager.class, new MyWarehouseManager());
        ...
    }
}

```

After building the extension, place the `${your_extension_name}-ext.jar` file into the Config.ext_dir directory (default is FE/lib), and then restart the FE.

Example FE log after startup:

```sh
2025-12-26 12:47:46.047+08:00 INFO (main|1) [ExtensionManager.loadExtensionsFromDir():39] start to load extensions
2025-12-26 12:47:46.152+08:00 INFO (main|1) [ExtensionManager.loadExtensions():63] Loaded extension: extension_name
2025-12-26 12:47:46.152+08:00 INFO (main|1) [ExtensionManager.loadExtensionsFromDir():42] all extensions loaded finished
```

**Explanation:**

- `start to load extensions`: FE has started scanning the extension directory.
- `Loaded extension: extension_name`: The extension was successfully loaded.
- `all extensions loaded finished`: All extensions in the directory have been loaded.
