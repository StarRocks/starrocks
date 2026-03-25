---
displayed_sidebar: docs
sidebar_label: "Hadoop Wildfly Native SSL ライブラリ問題"
sidebar_position: 99
---
# FAQ：StarRocks における Hadoop 3.4.3 Wildfly Native SSL ライブラリ問題

## 背景

StarRocks は、同梱する Hadoop 依存関係を 3.4.2 から 3.4.3 にアップグレードしました（[PR #69503](https://github.com/StarRocks/starrocks/pull/69503)）。Hadoop 3.4.3 には [HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719) が含まれており、OpenSSL 3.0 への対応のために Wildfly OpenSSL バインディングが `2.1.4.Final` から `2.2.5.Final` に更新されています。しかし、新しいネイティブライブラリ（`libwfssl.so`）は **GLIBC 2.34+** を前提にビルドされており、古い Linux ディストリビューションでは互換性の問題が発生します。

**影響を受ける StarRocks バージョン：** 4.1.0+、4.0.7+、3.5.14+

### 典型的な症状

この問題が発生すると、CN/BE ノードが SSL コンテキスト初期化時に **SIGSEGV（セグメンテーションフォルト）** でクラッシュすることがあります。FE では以下のようなエラーが報告されます：

```
SQL Error [1064] [42000]: Access storage error. Error message: failed to get file schema:
A error occurred: errorCode=2001 errorMessage:Channel inactive error!
```

[GitHub Issue #70478](https://github.com/StarRocks/starrocks/issues/70478) では、`FILES()` 関数を使用して Azure Data Lake 上の Parquet ファイルをクエリした際にこの問題が発生しています。

```
*** SIGSEGV (@0x0) received by PID (TID 0x...) ***
    @     0x7b9b67093453 SSL_CTX_new_ex
    @     0x7b9b6a09d95a Java_org_wildfly_openssl_SSLImpl_makeSSLContext0
    @     0x7b9a68544be1 (unknown)
```

原因は、Wildfly の JNI 呼び出し `Java_org_wildfly_openssl_SSLImpl_makeSSLContext0` が OpenSSL のネイティブコードを実行する際、同梱された `libwfssl.so` とシステムの OpenSSL との間で ABI／ランタイムの不整合が発生し、クラッシュするためです。

---

## Q1：Wildfly OpenSSL ネイティブライブラリとは何ですか？

Hadoop は [Wildfly OpenSSL](https://github.com/wildfly-security/wildfly-openssl) を使用して、ネイティブ OpenSSL を Java の JSSE（Java Secure Socket Extension）API にバインドしています。これにより、Hadoop の S3A、Azure Blob（ABFS）、Azure Data Lake（ADL）コネクタは、JVM 内蔵の SSL 実装ではなく、ネイティブ OpenSSL を用いて TLS/SSL 接続を処理できます。正常に動作する場合、高スループットな暗号化 I/O において性能向上が期待できます。

---

## Q2：Hadoop 3.4.3 では何が問題ですか？

Hadoop 3.4.3 は OpenSSL 3.0 対応のため、Wildfly OpenSSL を `2.2.5.Final` にアップグレードしました（[HADOOP-19719](https://issues.apache.org/jira/browse/HADOOP-19719)）。しかし、新しいネイティブライブラリ（`libwfssl.so`）は **GLIBC 2.34+** でコンパイルされており、以下のような問題が発生します：

| プラットフォーム                           | GLIBC バージョン | OpenSSL バージョン | 障害内容                                                           |
| ---------------------------------- | ----------- | ------------- | -------------------------------------------------------------- |
| **RHEL 8 / CentOS 8**              | 2.28        | 1.1.1         | `UnsatisfiedLinkError: GLIBC_2.34 not found` によりライブラリがロードできない  |
| **Ubuntu 22.04**（`libssl-dev` 未導入） | 2.35        | 3.0.2         | ライブラリはロードされるが、「file not found」や「interface not supported」エラーが発生 |
| **RHEL 9 / Ubuntu 24.04**          | 2.34+       | 3.0+          | 通常は動作するが、OpenSSL 開発ヘッダがないと問題が発生する可能性あり                         |

影響を受ける環境では、以下の問題が発生する可能性があります：

- JVM の不安定化（CN/BE ノードで `hs_err` クラッシュログが生成される）
- クラウドストレージ（AWS S3、Azure Blob、Azure Data Lake）への SSL/TLS 接続失敗
- 暗号化されていない接続や異常な接続へのサイレントフォールバック

---

## Q3：どのように対処すればよいですか？

環境に応じて、以下の解決策を選択できます。

### 解決策 1：Wildfly Native SSL を無効化（推奨）

Hadoop に JVM 内蔵の SSL 実装（JSSE）を使用させ、Wildfly/OpenSSL のネイティブ経路を無効化します。本方法は**最も安全で移植性が高い**対策です。

すべての CN/BE ノードの `core-site.xml` に設定を追加してください。

```xml
<configuration>
    <!-- Disable Wildfly native SSL for AWS S3 (S3A connector) -->
    <property>
        <name>fs.s3a.ssl.channel.mode</name>
        <value>default_jsse</value>
    </property>

    <!-- Disable Wildfly native SSL for Azure Data Lake (ADL connector) -->
    <property>
        <name>adl.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>

    <!-- Disable Wildfly native SSL for Azure Blob Storage (ABFS connector) -->
    <property>
        <name>fs.azure.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>
</configuration>
```

**`core-site.xml` の配置場所：**
- StarRocks BE/CN：`$STARROCKS_HOME/conf/core-site.xml`
- Broker プロセス：`$BROKER_HOME/conf/core-site.xml`

設定後は、**すべての関連サービスを再起動**してください。

---

### 解決策 2：OpenSSL 開発ヘッダのインストール

Ubuntu 22.04 以降では、OpenSSL の開発パッケージをインストールすることで問題が解消される場合があります：

```bash
sudo apt install libssl-dev
```

---

## Q4：どのクラウドストレージコネクタが影響を受けますか？

| コネクタ                     | 設定項目                        | 推奨値            |
| ------------------------ | --------------------------- | -------------- |
| **AWS S3（S3A）**          | `fs.s3a.ssl.channel.mode`   | `default_jsse` |
| **Azure Data Lake（ADL）** | `adl.ssl.channel.mode`      | `Default_JSSE` |
| **Azure Blob（ABFS）**     | `fs.azure.ssl.channel.mode` | `Default_JSSE` |

:::note
Azure Data Lake Store SDK は `AdlStoreOptions.setSSLChannelMode()` によるプログラム設定も可能ですが、StarRocks では `core-site.xml` による設定が標準的です。
:::

---

## Q5：Native SSL を無効化すると性能に影響はありますか？

影響はありますが、多くのワークロードでは軽微です。

- Wildfly + OpenSSL（ネイティブ経路）は、大規模な暗号化 I/O において高いスループットを提供します
- JVM の JSSE 実装は、最新の JDK（Java 11 以降）では十分に最適化されており、多くの StarRocks のデータレイククエリには十分です

RHEL 9 や Ubuntu 24.04（GLIBC 2.34+、OpenSSL 3.0）などの対応環境では、最大性能のために `default` または `openssl` モードを使用することも可能です。

---

## Q6：修正が有効かどうかを確認するには？

設定変更およびサービス再起動後、以下を確認してください：

1. **問題が発生していた処理を再実行**（例：外部 S3/Azure テーブルへのクエリ）
2. **BE/CN ログを確認**し、以下が出力されていないことを確認：

   - `hs_err_pid*.log`（JVM クラッシュログ）
   - `GLIBC_2.34` や `libwfssl.so` を含む `UnsatisfiedLinkError`
   - `org.wildfly.openssl` に関連するスタックトレース
3. **SSL 接続が正常に確立されていることを確認**：

   - `default_jsse` 使用時：標準的な JSSE ハンドシェイクログ（Wildfly 関連ログなし）
   - `default` 使用時：`Failed to load OpenSSL. Falling back to the JSSE` が出る場合あり（正常動作）

---

## Q7：RHEL 8 / CentOS 8 では特に問題になりますか？

はい。

RHEL 8 は **GLIBC 2.28** を使用しており、必要な 2.34 より大幅に古いため：

- ネイティブライブラリは**ロードできません**
- Hadoop 3.4.3 のリリースノートでも、「RHEL8 では動作しないため JVM の SSL を使用すること」と明記されています

**推奨：**

- RHEL 8 では常に `default_jsse` を使用
- 長期的には RHEL 9 への移行を検討

---

## Q8：FIPS 準拠環境ではどうなりますか？

影響を受ける可能性があります。

FIPS 準拠の SSL ライブラリを使用している Linux 環境では、Wildfly OpenSSL のネイティブバインディングと互換性がない場合があります。

そのため：

- **原則として `default_jsse`（JVM SSL）を使用してください**
- ネイティブライブラリの互換性が確認されている場合を除き、使用は推奨されません

---

## Q9：StarRocks 側での対策は？

1. **Docker イメージの修正**（[PR #70688](https://github.com/StarRocks/starrocks/pull/70688)）
   Ubuntu ランタイムイメージに `libssl-dev` を追加
2. **ドキュメント整備**
   本 FAQ および `core-site.xml` 設定ガイドを提供

---

## クイックリファレンス：最小対応

すべての CN/BE ノードの `$STARROCKS_HOME/conf/core-site.xml` に設定を追加し、サービスを再起動してください。

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.s3a.ssl.channel.mode</name>
        <value>default_jsse</value>
    </property>
    <property>
        <name>adl.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>
    <property>
        <name>fs.azure.ssl.channel.mode</name>
        <value>Default_JSSE</value>
    </property>
</configuration>
```

---

## 参考資料

- [HADOOP-19719: Upgrade to wildfly version with support for openssl 3](https://issues.apache.org/jira/browse/HADOOP-19719)
- [HADOOP-19262: Upgrade wildfly-openssl for JDK 17+](https://issues.apache.org/jira/browse/HADOOP-19262)
- [FLINK-38284: Flink downstream tracking issue](https://issues.apache.org/jira/browse/FLINK-38284)
- [Hadoop S3A Troubleshooting Guide](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/troubleshooting_s3a.html)
- [Hadoop S3A Performance Guide (SSL Channel Mode)](https://apache.github.io/hadoop/hadoop-aws/tools/hadoop-aws/performance.html)
- [Wildfly OpenSSL GitHub Repository](https://github.com/wildfly-security/wildfly-openssl)
- [StarRocks Issue #70478: SIGSEGV on Azure ADLS2 query (4.0.7)](https://github.com/StarRocks/starrocks/issues/70478)
- [StarRocks PR #69503: Upgrade Hadoop 3.4.2 to 3.4.3](https://github.com/StarRocks/starrocks/pull/69503)
- [StarRocks PR #70688: Install libssl-dev for Ubuntu runtime](https://github.com/StarRocks/starrocks/pull/70688)
