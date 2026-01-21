---
displayed_sidebar: docs
---

# 基于 macOS ARM64 编译、运行和测试 StarRocks

本文档详细介绍如何在 macOS ARM64 平台（Apple Silicon）上编译、运行、调试和测试 StarRocks，方便开发者在 Mac 上进行开发和调试工作。

## 前提条件

### 软件要求

- macOS 15.0 或更高版本
- Xcode Command Line Tools
- Homebrew 包管理器

### 安装基础工具

1. 安装 Xcode Command Line Tools：

```bash
xcode-select --install
```

2. 安装 Homebrew（如果尚未安装）：

```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

3. 安装必要的 Homebrew 依赖：

```bash
cd build-mac
./env_macos.sh
```

## Mac 编译

## 编译第三方库

在 macOS 上编译 BE 之前，需要先编译第三方依赖库。进入 `build-mac` 目录并运行：

```bash
cd build-mac
./build_thirdparty.sh
```

## 编译 FE

### 编译步骤

```bash
./build.sh --fe
```

### 编译输出

编译完成后，FE 产物位于：

```
output/fe/
├── bin/              # FE 启动脚本
├── conf/             # 配置文件
├── lib/              # JAR 包和依赖
└── meta/             # 元数据存储目录
```

## 编译 BE

macOS 版本的 BE 编译脚本位于 `build-mac` 目录。

### 首次编译

```bash
cd build-mac
./build_be.sh
```

首次编译会自动完成以下步骤：

1. 检查和配置环境变量
2. 编译第三方依赖（protobuf、thrift、brpc 等）
3. 生成代码（Thrift/Protobuf）
4. 编译 BE 代码
5. 安装到 `be/output/` 目录

### 编译输出

编译完成后，BE 产物位于：

```
output/be/
├── bin/              # 启动脚本
├── conf/             # 配置文件
├── lib/              # starrocks_be 二进制文件
│   └── starrocks_be  # BE 主程序
└── storage/          # 数据存储目录
```

## 配置和运行 FE

### 配置 FE

`build-mac/start_fe.sh` 会自动写入必要的默认配置（如 `priority_networks`、`default_replication_num`、`bdbje_reset_election_group`）。如需自定义，请修改 `output/fe/conf/fe.conf`。

### 启动 FE

```bash
cd build-mac
./start_fe.sh --daemon
```

### 查看 FE 日志

```bash
tail -f output/fe/log/fe.log
```

## 配置和运行 BE

### 配置 BE

`build-mac/start_be.sh` 会自动设置运行所需环境变量，并写入必要的默认配置（如 `priority_networks`、`datacache_enable`、`enable_system_metrics`、`sys_log_verbose_modules`）。如需自定义，请修改 `output/be/conf/be.conf`。

### 启动 BE

```bash
cd build-mac
./start_be.sh
```

### 查看 BE 日志

```bash
tail -f output/be/log/be.INFO
```

### 停止 BE

```bash
ps aux | grep starrocks_be

kill -9 <PID>
```

### 将 BE 添加到集群

启动 BE 后，需要通过 MySQL 客户端将其注册到 FE：

```sql
-- 连接到 FE
mysql -h 127.0.0.1 -P 9030 -u root

-- 添加 BE（替换为你的实际 IP 地址）
ALTER SYSTEM ADD BACKEND "127.0.0.1:9050";

-- 查看 BE 状态
SHOW BACKENDS\G
```

确认 `Alive` 字段为 `true` 表示 BE 运行正常。

## 连接和测试

### 使用 MySQL 客户端连接

```bash
# 使用 Homebrew 安装的 MySQL 客户端
/opt/homebrew/Cellar/mysql-client/9.4.0/bin/mysql -h 127.0.0.1 -P 9030 -u root

# 或使用系统路径（如果已配置）
mysql -h 127.0.0.1 -P 9030 -u root
```

### 基本测试

```sql
-- 创建数据库
CREATE DATABASE test_db;
USE test_db;

-- 创建表
CREATE TABLE test_table (
    id INT,
    name VARCHAR(100)
) DISTRIBUTED BY HASH(id) BUCKETS 1;

-- 插入数据
INSERT INTO test_table VALUES (1, 'Alice'), (2, 'Bob');

-- 查询数据
SELECT * FROM test_table;
```

## 调试 BE

### 使用 LLDB 调试

macOS 使用 LLDB 作为调试器（GDB 的替代品）。

#### 附加到运行中的 BE 进程

```bash
# 查找 BE 进程 ID
ps aux | grep starrocks_be

# 使用 LLDB 附加到进程（替换 <PID> 为实际进程 ID）
sudo lldb -p <PID>
```

#### 常用 LLDB 命令

```lldb
# 查看所有线程
thread list

# 查看当前线程的堆栈
bt

# 查看所有线程的堆栈
thread backtrace all

# 继续执行
continue

# 单步执行
step

# 设置断点
breakpoint set --name function_name

# 退出 LLDB
quit
```

#### 导出所有线程堆栈到文件

```bash
# 导出堆栈信息用于问题分析
lldb -p <PID> --batch -o "thread backtrace all" > starrocks_bt.txt
```

## SQL 测试

StarRocks 提供了完整的 SQL 测试框架，可以在 macOS 上运行。

### 准备测试环境

```bash
# 进入测试目录
cd test

# 创建 Python 虚拟环境
python3 -m venv venv

# 激活虚拟环境
source venv/bin/activate

# 安装测试依赖
python3 -m pip install -r requirements.txt
```

### 标记 macOS 兼容的测试用例

由于 macOS 版本禁用了部分功能，需要标记哪些测试用例可以在 Mac 上运行：

```bash
# 标记 test_scan 目录的测试（预览模式，不实际修改）
python3 tag_mac_tests.py -d ./sql/test_scan --dry-run

# 实际标记测试
python3 tag_mac_tests.py -d ./sql/test_scan

# 标记 test_agg 目录的测试
python3 tag_mac_tests.py -d ./sql/test_agg
```

### 运行 SQL 测试

```bash
# 运行带 @mac 标签的测试
pytest sql/test_scan -m mac

# 运行特定测试文件
pytest sql/test_scan/test_basic_scan.py -m mac

# 详细输出
pytest sql/test_agg -m mac -v
```

### 测试配置

测试框架会自动连接到本地的 StarRocks 实例：

- Host: `127.0.0.1`
- Port: `9030`
- User: `root`
- Password: 空

## 开发工具配置

### Git Pre-commit Hook（代码格式化）

为了保持代码风格一致，建议配置 pre-commit hook 自动格式化 C++ 代码。

创建文件 `.git/hooks/pre-commit`：

```bash
#!/bin/bash

echo "Running clang-format on modified files..."

STYLE=$(git config --get hooks.clangformat.style)
if [ -n "${STYLE}" ] ; then
  STYLEARG="-style=${STYLE}"
else
  STYLEARG=""
fi

format_file() {
  file="${1}"
  if [ -f $file ]; then
    /opt/homebrew/Cellar/clang-format@11/11.1.0/bin/clang-format-11 -i ${STYLEARG} ${1}
    git add ${1}
  fi
}

case "${1}" in
  --about )
    echo "Runs clang-format on source files"
    ;;
  * )
    for file in `git diff-index --cached --name-only HEAD | grep -iE '\.(cpp|cc|h|hpp)$' ` ; do
      format_file "${file}"
    done
    ;;
esac
```

添加执行权限：

```bash
chmod +x .git/hooks/pre-commit
```

## 限制和已知问题

### macOS 版本禁用的功能

由于平台兼容性限制，以下功能在 macOS 版本中被禁用：

#### 存储和数据源

- **HDFS 支持**：无 HDFS 客户端库
- **ORC 格式**：完整 ORC 模块已禁用
- **Avro 格式**：avro-c 库兼容性问题
- **JDBC 数据源**：JNI/Java 依赖
- **MySQL Scanner**：缺少 MariaDB 开发头文件

#### 云服务集成

- **AWS S3**：Poco HTTP 客户端兼容性问题
- **Azure 存储**：Azure SDK 兼容性问题
- **Apache Pulsar**：librdkafka++ 兼容性问题

#### 性能和监控工具

- **Google Breakpad**：崩溃报告（minidump）
- **OpenTelemetry**：分布式追踪

#### 其他功能

- **GEO 模块**：缺少生成的语法解析文件
- **JIT 编译**：需要完整 LLVM 开发环境
- **Starcache**：缓存加速功能
- **CLucene**：倒排索引功能
- **LZO 压缩**：使用替代压缩算法

### 设计原则

macOS 版本编译的实现遵循以下原则：

1. **不影响 Linux 编译**：所有修改通过条件编译隔离
2. **最小化代码修改**：优先通过配置禁用功能
3. **集中化管理**：Mac 相关修改集中在 `build-mac/` 目录
4. **使用标准工具**：依赖 Homebrew 和 LLVM 工具链
5. **源码编译关键依赖**：确保 ABI 兼容性（protobuf、thrift、brpc）



## 常见问题解答

**Q: 编译时报错 "protobuf version mismatch"**

A: 确保使用 `thirdparty/installed/bin/protoc` (版本 3.14.0)，而不是系统或 Homebrew 的 protobuf：

```bash
# 检查 protobuf 版本
/Users/kks/git/starrocks/thirdparty/installed/bin/protoc --version

# 应该输出：libprotoc 3.14.0
```

**Q: 编译时内存不足**

A: 减少并行编译任务数：

```bash
./build_be.sh --parallel 4
```

**Q: FE 无法连接 BE**

A: 确保 FE 和 BE 的 `priority_networks` 配置一致，且都包含本机 IP 地址。

**Q: 某些测试用例失败**

A: 检查是否是禁用功能相关的测试（如 HDFS、S3、ORC 等），这些测试在 macOS 上无法通过。

## 贡献指南

如果你在 macOS 上发现问题或有改进建议：

1. 检查 `build-mac/` 目录中的相关脚本
2. 遵循"不影响 Linux 编译"的原则
3. 使用 `#ifdef __APPLE__` 进行平台相关的代码修改
4. 提交 Pull Request 并详细说明修改内容

欢迎从以下方面贡献：

- 支持在 mac 上编译更多被禁用的功能
- 持续完善 sql test的 @mac 标签

---

Happy coding on macOS! 🍎
