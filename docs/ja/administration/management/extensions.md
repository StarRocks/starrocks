---
displayed_sidebar: docs
---

# 静的拡張機能を開発する

静的拡張機能は、StarRocks FE の拡張モジュールであり、コアコードを変更することなく新機能を追加したり、既存の機能を最適化したりできます。動的プラグインと比較して、静的拡張機能はシステム起動時に自動的にロードされ、システムのコアモジュールをカバーする、より多くの登録可能な拡張ポイントを提供します。

この機能は v4.1 以降でサポートされています。

## 使用方法

次の例は、静的拡張機能を開発する方法を示しています。

### 前提条件

StarRocks FE 開発環境を次のように準備します。

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

### 拡張機能のエントリポイント

```java
@SRModule(name = "extension_name")
public class MultiWarehouseExtension implements StarRocksExtension {
    @Override
    public void onLoad(ExtensionContext ctx) {
        // 独自のクラスを登録するか、その他の初期化を実行します
        ctx.register(WarehouseManager.class, new MyWarehouseManager());
        ...
    }
}

```

### ログ

拡張機能をビルドした後、`${your_extension_name}-ext.jar` ファイルを `Config.ext_dir` ディレクトリ（デフォルトは `FE/lib`）に配置し、FE を再起動します。

起動後の FE ログの例は次のとおりです。

```sh
2025-12-26 12:47:46.047+08:00 INFO (main|1) [ExtensionManager.loadExtensionsFromDir():39] start to load extensions
2025-12-26 12:47:46.152+08:00 INFO (main|1) [ExtensionManager.loadExtensions():63] Loaded extension: extension_name
2025-12-26 12:47:46.152+08:00 INFO (main|1) [ExtensionManager.loadExtensionsFromDir():42] all extensions loaded finished
```

- `start to load extensions`: FE は拡張ディレクトリのスキャンを開始しました。
- `Loaded extension: extension_name`: 拡張機能は正常にロードされました。
- `all extensions loaded finished`: ディレクトリ内のすべての拡張機能がロードされました。
