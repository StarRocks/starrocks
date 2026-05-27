---
displayed_sidebar: docs
keywords: ['FE', 'Coordinator Node','memory', 'OOM', 'JVM', 'troubleshooting', 'heap']
---

# FE / コーディネーターノードのメモリ不足問題のトラブルシューティング

「FEメモリ不足」の問題を防ぎ、迅速に回復し、再発防止のためにできる限り原因を特定します。

:::note

- この情報はFEノードおよびコーディネーターノード（共有データデプロイメント）に適用されます。読みやすさのために、両方のタイプのフロントエンドノードを「FE」と呼びます。
- この記事では主にヒープメモリの状況と解決策を分析しており、オフヒープメモリについては扱いません。OOMがオフヒープメモリの問題によって引き起こされている場合は、JVM XMXの設定を減らすか、マシンのメモリを拡張することを検討してください。

:::

## FEメモリ構造と割り当ての推奨事項

### FEメモリの構成

フロントエンドサービス（FE）はJVM上で動作し、そのメモリ消費はJavaヒープとオフヒープメモリのオーバーヘッドから成ります。

#### JVMメモリモジュールの概要

FEプロセス中にJVMを通じて割り当てられるすべてのメモリは、大まかに以下のモジュールに分けられます：

| モジュール | タイプ | 説明 |
|:--------|-------------|:------|
| **Javaヒープ** | ヒープ | Javaオブジェクトインスタンスのメモリ（プランキャッシュ、メタデータ、一時オブジェクトなど） |
| **クラス** | オフヒープ | クラスのロード時に生成されるメタデータなどの構造 |
| **スレッド** | オフヒープ | スレッドが占有するメモリは主に各スレッドのスタック空間です（通常、スレッドあたり約1MB）。 |
| **コード** | オフヒープ | JVMがバイトコードをネイティブマシンコードにコンパイルする際に占有するメモリ |
| **GC** | オフヒープ | ガベージコレクターの内部作業データ構造 |
| **コンパイラ** | オフヒープ | HotSpotコンパイラが使用するメモリはCodeと同程度になる場合があります。 |
| **その他** | オフヒープ | Direct ByteBuffer、Nettyバッファなどのダイレクトメモリを含む |
| **シンボル** | オフヒープ | 文字列定数プール、整数定数プールなどのSymbolTable構造 |
| **ネイティブメモリトラッキング（NMT）** | オフヒープ | NMT自身が記録するメモリ使用量が占有するメモリ |
| **アリーナチャンク** | オフヒープ | JVMが一時的な保存と再利用のために内部で使用する一時メモリブロック（mallocを使用して割り当て） |
| **ロギング** | オフヒープ | ロギングシステム（log4jなど）が占有するメモリ |
| **引数** | オフヒープ | JVM起動パラメーターの受け渡しが占有するメモリ |
| **モジュール** | オフヒープ | Javaモジュールシステムで使用される構造 |

JVMおよびStarRocksの動作特性を考慮すると、FEのメモリは主に以下の部分で構成されます：

1. **JVMヒープメモリ（ヒープ）**
   - Javaオブジェクトインスタンス（Plan Cache、メタデータキャッシュなど）を格納します。
   - 最大サイズはパラメータ `-Xmx` で設定され、ガベージコレクションメカニズムに大きく影響されます。

2. **非ヒープメモリ（Metaspace）**
   - ストレージクラス定義、定数プールなどのメタデータ。
   - Java 8以降でPermGenに置き換わりました。

3. **ダイレクトメモリ**
   - Netty、ByteBuffer、ログバッファ、RPCフレームワークなどが使用するオフヒープメモリ。
   - `sun.misc.Unsafe` によって割り当てられ、通常はGCによって制御されません。

4. **スレッドスタックメモリ**
   - 各Javaスレッドは作成時にスレッドスタックを要求し、デフォルトサイズは約1MB（`-Xss` で設定可能）です。
   - スレッド数が多い場合（高並列シナリオなど）、メモリの積み上がりが発生しやすくなります。

5. **JNI / ネイティブメモリ呼び出し**
   - データベースは、ロギングコンポーネント（RocksDB、Nettyなど）、Cacheなどを通じてネイティブレイヤーを呼び出す際にメモリを消費する場合があります。

### JVMパラメータ設定の推奨事項

JVMでは、ヒープメモリは `Xmx` で設定します。一般的に、プロセスが使用するメモリが `Xmx` の130%未満であることが適切です。つまり、JVM `Xmx` で設定されたメモリに加えて、一部のオフヒープメモリも使用されます。例えば、JVMに21gを設定した場合、`top` で確認できるプロセスのRSSメモリ使用量は約27gになります。

経験に基づくと、タブレット数とFEメモリの推奨関係は以下の通りです：

| タブレット数 | 推奨FEメモリ設定 |
|:-------------|:------------------------------------|
| 1M未満 | 16 GB                               |
| 1M - 2M      | 32 GB                               |
| 2M - 5M      | 64 GB                               |
| 5M - 10M     | 128 GB                              |

独立デプロイ環境では、システムに十分なメモリを確保することを推奨します。システム用に確保するメモリに加えて、オフヒープメモリも考慮する必要があります。

混合デプロイ環境では、OOMを防ぐために、他のサービスの使用状況を考慮した上で、できる限り十分なメモリを確保する必要があります。

FEを独立デプロイする場合（混合デプロイは推奨しません）：

1. マシンのメモリが32G未満の場合、`-Xmx`（最大ヒープサイズ）はマシンメモリの最大70%に設定してください。
2. マシンのメモリが32Gを超える場合、`-Xmx`（最大ヒープサイズ）はマシンメモリの最大80%に設定してください。
3. FEリーダーノードはチェックポイントを実行する必要があるため、一般的にリーダーノードのメモリはフォロワーノードの約2倍になります。

バージョン3.4以降では、リーダーはフォロワーノードを選択してチェックポイントを実行し、フォロワーの結果をダウンロードして他のフォロワーに配布します。リーダーのメモリ使用量はフォロワーに近くなります。参照：[PR #52103](https://github.com/StarRocks/starrocks/pull/52103)。

### FE JVMモニタリングとメモリプロファイルのデプロイ

FEの状態において、ヒープモニタリングとGCモニタリングはサービスの状態を効果的に監視でき、事後の問題解決における重要な手がかりにもなります。タイムリーにモニタリングを設定し、必要に応じて対応するアラートを設定することを推奨します。

具体的なモニタリング指標は以下の通りです：

| 指標                  | 説明                             |
|:---------------------------|:----------------------------------------|
| `jvm_heap_size_bytes`      | ヒープメモリ使用量。                      |
| `jvm_non_heap_size_bytes`  | 主にmetaspace、コードキャッシュなどを含み、すべてのオフヒープメモリは含みません。 |
| `jvm_old_gc{type="count"}` | Full GC回数。                          |
| `jvm_old_gc{type="time"}`  | Full GC時間。                           |

これらのメトリクスおよびその他のモニタリング情報は[モニタリングとアラート](../administration/management/monitoring/Monitor_and_Alert.md)

メモリプロファイルは突発的なヒープ増加を分析できます。サービスのインストール完了後、正常に動作するかどうかを確認する必要があります。

3.3.6以降では、データベースは定期的にメモリプロファイルを出力し、`log/proc_profile` にHTML形式でtgz圧縮して保存します。

制御設定：

```properties
proc_profile_collect_interval_s = 600
proc_profile_collect_time_s = 300
proc_profile_cpu_enable = true
proc_profile_mem_enable = true
```

3.3.6より前のバージョンでは、スクリプトを使用して定期的にプロファイルを出力できます。各FEのインストールディレクトリ配下に `mem_profiler.sh` という名前の新しいスクリプトファイルを作成し、以下の内容をファイルにコピーしてください。

:::note

このスクリプトはFEプロセスに影響を与えず、サービスの通常使用に影響しません。また、48時間経過したファイルを自動的にクリーンアップします。

:::

```bash
#!/bin/bash
mkdir -p mem_alloc_log

cleanup_old_files() {
    find mem_alloc_log -name "alloc-profile-*.html" -mmin +2880 -exec rm -f {} \;
}

while true
do
    cleanup_old_files

    current_time=$(date +'%Y-%m-%d-%H-%M-%S')
    file_name="mem_alloc_log/alloc-profile-${current_time}.html"
    ./bin/profiler.sh -e alloc --alloc 2m -d 300 -f "$file_name" `cat bin/fe.pid`
done
```

```bash
# バックグラウンド起動
chmod +x mem_profiler.sh
nohup ./mem_profiler.sh > mem_profiler.out 2>&1 &

# プロセスが存在するか確認する
ps aux | grep mem_profiler.sh

# プロセスを停止する
pkill -f mem_profiler.sh
```

## FEクラッシュ

FEクラッシュの原因は大きく4種類に分けられます。

### 1. FEプロセスのメモリ使用量が高く、オペレーティングシステムによって強制終了された

システムログを照会することで、これが原因かどうかを確認できます：

```bash
# dmesgを確認する
dmesg | grep -iE 'out of memory|oom|kill|killed process'

# メッセージログを表示する
sudo grep -Ei 'killed process|oom.kill|sending SIG' -C5 /var/log/messages /var/log/syslog 2>/dev/null | tail -n 100
```

OOMによって強制終了された後、ログにはシステムプロセスのメモリ使用量が出力される場合があります。OOMによって表示される`total_vm`や`rss`などのフィールドはページ単位であり、デフォルトでは1ページ = 4KBです：

- `total_vm = 8793525` → 約8793525 × 4KB ≈ 33.5 GBの仮想メモリを要求したことを示します
- `rss = 7218120` → 約27.5 GBの物理メモリが実際に使用されており、それは7218120 × 4KBであることを示します

OOMログの例：

```
Jun 10 11:20:24 tp-prod-bigdata-sr-ss-fe-2-b kernel: [ pid ]  uid  tgid  total_vm     rss  nr_ptes  swapents  oom_score_adj  name
Jun 10 11:21:58 tp-prod-bigdata-sr-ss-fe-2-b kernel: [22654]  1005 22654   8793525 7218120    14943         0              0  java
```

実際のRSS使用量を確認してください。マシンのメモリに非常に近い場合、一般的にJVMの割り当てが不適切であることが原因です。[JVMパラメータ設定の推奨事項](#jvm-parameter-configuration-recommendations)セクションを参照してください。

### 2. FEリーダーのヒープメモリが高く、Full GCがトリガーされ、リーダーの切り替えが発生した

リーダーの切り替えにより旧リーダーが終了すると、`fe.out`に以下の情報が出力されます：

```
transfer FE type from LEADER to UNKNOWN. exit
```

または

```
transfer FE type from LEADER to FOLLOWER. exit
```

ほとんどのリーダー切り替えはFull GCによって引き起こされます。まれに、リーダーのCPU使用率が高い場合や`jstack`の実行によってトリガーされる場合があります。

**GCログの分析：**

GCログを[https://gceasy.io/](https://gceasy.io/)にアップロードし、**ポーズGCの時間**をクリックしてください。単一のポーズGCの時間が30秒を超える場合、または10秒を超えるポーズGCが頻繁に発生する場合、リーダーの切り替えがトリガーされます。

をクリックして**GC前のヒープ**ヒープメモリが上昇する時点を見つけてください。この時間範囲を使用して、その期間のメモリプロファイルファイルを特定し、その時点でどのモジュールがメモリを要求していたかを識別します。

### 3. FEプログラムの内部バグによってクラッシュが発生した

この状況はここでの説明の範囲外です。[メタデータの復元](../administration/Meta_recovery.md)ドキュメントを参照してください。

### 4. JVMのバグによってFEがクラッシュした

この状況は非常にまれです。発生した場合、`log/hs_err_pid%p.log`にあるクラッシュログを確認することで原因を調査できます。ここで`%p`はプロセスIDです。

## FEプロセスは正常だがメモリ使用量が高い

FEの長期運用中に、ヒープ内メモリ使用量が異常に増加し、Full GCが頻繁にトリガーされ、クエリやインポートが遅くなることがあります。問題はヒープの**急激な増加**または継続的な**緩やかな増加**として現れる場合があります。

### 現地調査のプロセス

監視ツールを開いてください：[監視とアラート](../administration/management/monitoring/Monitor_and_Alert.md)

**ステップ1：以下の監視を確認する**

- **FE JVM パネル**：JVMヒープメモリ使用量メトリック

**ステップ2：メモリ増加の種類を特定する**

| 種類 | 特徴 | その後のトラブルシューティングパス |
|------|---------|--------------------------------|
| **急激な増加** | ヒープメモリが短時間（秒〜分）で急激にスパイクする | QPS、接続数、SQLリクエストを優先的に確認する |
| **緩やかな上昇** | メモリが徐々に増加し、GC後の回復が悪化する。 | メモリリークまたはキャッシュの蓄積のトラブルシューティングを優先する |

### メモリスパイクの調査

短時間でリクエスト負荷が増加するかどうか — 高QPSまたは高並列メタデータ操作がメモリの急増につながる可能性があります。

**1. 以下のモニタリングを確認してください：**

- **QPS パネル**：FEが受信したリクエスト数が突然増加しましたか？
- **接続パネル**：FEの同時接続数が瞬間的にスパイクしましたか？

**2. 監査テーブルまたはログを通じて異常なSQLを確認する：**

過去5〜10分間の高頻度SQLステートメントを照会する：

```sql
SELECT * FROM starrocks_audit_db__.starrocks_audit_tbl__
WHERE feip = 'IP of memory abnormal node'
  AND `timestamp` >= now() - INTERVAL 1000 MINUTE
ORDER BY `timestamp` DESC;
```

特に注意すべき点：

- 深いサブクエリ、複数のJOIN、複雑な集計を含むステートメント
- 非標準的なビジネスSQL（BIプラットフォームが生成したデータセットの並列SQLなど）
- バージョン3.3.13以降では、FE監査ログの `queryFeMemory` フィールドを確認することができ、クエリがFEで合計どれだけのメモリを申請したかを示します。
- FE OOMによりFEプロセスが終了した場合、実行中のクエリと対応する `QueryFEAllocatedMemory` が自動的に出力されます。

FEログ内の検索キーワード：

```
QueryFEAllocatedMemory
```

**3. FEログを通じて大量のメタデータ操作（テーブルの作成と削除）を確認する：**

```bash
# テーブルを作成する
grep "Begin to unprotect create table" fe.log

# テーブルを削除する
grep "Finished drop table" fe.log
```

### メモリの緩やかな増加

**4. JVM に設定されたメモリが小さすぎる**

FEプロセスのGCステータスを監視してFull GCが頻繁に発生しているかどうかを確認します。`jstat` コマンドでJVMメモリ使用量を確認してください：

```bash
jstat -gcutil $pid 1000 1000
```

出力例：

```
 S0     S1     E      O      M     CCS    YGC     YGCT    FGC    FGCT     GCT
  0.00 100.00  27.78  95.45  97.77  94.45   24    0.226     1    0.065    0.291
  0.00 100.00  44.44  95.45  97.77  94.45   24    0.226     1    0.065    0.291
  0.00 100.00  55.56  95.45  97.77  94.45   24    0.226     1    0.065    0.291
```

`O`（旧世代）の割合が比較的高い状態が続いている場合、JVMメモリ設定に問題があることを示しており、JVMメモリを増やす必要があります。

**5. FEのメモリトラッカー（3.3.7以降）を通じてリークがあるかどうかを観察する**

`MemoryUsageTracker` のログを確認することで、どのモジュールにメモリリークがあるかを特定できます。このログは1時間ごとに各モジュールのメモリ消費量を出力します：

```
2025-06-06 16:37:50.633+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (6ms) Module Dict -
CacheDictManager estimated 0B of memory. Contains ColumnDict with 0 object(s).

2025-06-06 16:37:50.657+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (21ms) Module LocalMetastore -
LocalMetastore estimated 3.8MB of memory. Contains Partition with 17473 object(s).

2025-06-06 16:37:50.667+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():161] (0ms) Module TabletInvertedIndex -
TabletInvertedIndex estimated 37.7MB of memory. Contains TabletMeta with 353459 object(s).

2025-06-06 16:37:50.741+08:00 INFO (MemoryUsageTracker|74)
[MemoryUsageTracker.trackMemory():108] total tracked memory: 46.8MB,
jvm: Process used: 18.6GB, heap used: 4.4GB, non heap used: 289.1MB, direct buffer used: 395.5MB
```

最近のログエントリ間で各モジュールのメモリ増加を比較し、どのモジュールが増加しているかを特定します。

### 現場情報の収集

**プロセスがまだ再起動されていない場合は、直ちに `jmap` 情報を収集してください。**

#### jmap メモリトラブルシューティングプロセス

**ステップ1：FEのプロセスPIDを確認する**

```bash
ps aux | grep FE
```

**ステップ2：jmapを使用する**

:::warning[注意事項]

- `jmap -dump` はFEプロセスを短時間一時停止させます。オンラインの高い安定性が求められる場合は慎重に使用し、事前に通知することを推奨します。
- `jmap -histo` は通常プロセスへの影響はありません。GCのトリガーには数十ミリ秒かかる場合があります。
- ヒープファイルのエクスポートに `-dump` を頻繁に使用することは推奨されません。大きなオーバーヘッドが発生します。
- `histo` は大きなオブジェクトの予備判断に十分であり、必ずしもダンプを必要としません。

:::

**`jmap -histo:live`（本番環境では注意して使用してください）**

GCを強制することで、現在のヒープ内オブジェクトの分布をキャプチャします。`live` パラメータを使用すると、高メモリ使用量の問題が解決される場合があります：

```bash
jmap -histo:live <pid> > jmap_histo_$(date +%s).log
```

2つのサンプルを取得して差異を比較することをお勧めします：

```bash
jmap -histo:live <pid> > histo_1.log
sleep 60
jmap -histo:live <pid> > histo_2.log
```

**`jmap -histo pid`（軽量で、Full GCをトリガーしません）**

```bash
# 現在のJVMヒープ内のすべてのオブジェクトを取得します。これには
# すでにクリーンアップされたオブジェクトが含まれる場合があり、結果の分析に影響を与える可能性があります。
jmap -histo <pid> > histo.txt
```

**ステップ3：上位Nオブジェクトの占有率を分析する**

ファイルの先頭エントリを表示する：

```bash
head -n 30 histo_2.log
```

フィールドの説明：

| 列         | 意味           |
|------------|----------------|
| num        | クラス番号     |
| #instances | インスタンス数 |
| #bytes     | 合計バイト数   |
| class name | オブジェクト名 |

`histo_1` と `histo_2` を比較して、インスタンス数またはメモリが大幅に増加したクラスを特定します。

例：`java` で始まるクラスは、ビジネスクラスから参照されるユーティリティクラスです。上から下に見ていくと、前方にある関連クラスがより大きな割合を占めています。例えば、`com.starrocks.lake.LakeTablet` が大量のメモリを占有している場合、タブレットの過剰使用を示しています。

**ステップ4：完全なヒープダンプをエクスポートする（histoが不十分な場合）**

```bash
# Full GCをトリガーします。使用には注意が必要です。
jmap -dump:live,format=b,file=heap_$(date +%s).hprof <pid>
```

このファイルは非常に大きく（数GB）、**MAT** または **VisualVM** を使用して開いて分析する必要があります。

**ステップ5：OOM時の自動ダンプを設定する**

FEがメモリ不足になったときに自動的にダンプファイルを生成するために、`fe.conf` のJVM設定に以下を追加します：

```bash
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true \
  -Xmx8192m \
  -XX:+UseG1GC \
  -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time \
  -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=${LOG_DIR}/heap_dump_oom.hprof \
  -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy \
  -Djava.security.krb5.conf=/etc/krb5.conf \
  -Dsun.security.krb5.debug=true \
  -Dsun.security.spnego.debug=true \
  -Dcom.sun.management.jmxremote \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8113"
```

注意：`HeapDumpPath` はファイル名ではなく、ディレクトリパスを指定します。

#### 分析用のメモリプロファイルファイルを保持する

バージョン3.3.6以降、メモリプロファイルは `log/proc_profile` ディレクトリにHTML形式で出力され、tgzで圧縮されます。

メモリ増加の時刻に対応するファイルを見つけ、解凍してブラウザで開くと、メモリ割り当てのフレームグラフが表示されます。スタックの幅が広い部分ほど、より多くのメモリが割り当てられています。

メモリ増加の期間中にメモリプロファイルが出力されない場合は、CPUプロファイルをオフにして、出力間隔を5分に調整できます：

```properties
proc_profile_cpu_enable = false
proc_profile_collect_interval_s = 300
```

これらは動的パラメータであり、再起動せずに直接変更できます：

```sql
ADMIN SET FRONTEND CONFIG ("proc_profile_cpu_enable" = "false");
ADMIN SET FRONTEND CONFIG ("proc_profile_collect_interval_s" = "300");
```

## 緊急復旧

**1. FEサービスを停止する**

**2. ヒープメモリの設定をより大きな値に調整して再起動する**

`fe.conf` 内の `-Xmx` の値を変更する：

```bash
JAVA_OPTS="-Dlog4j2.formatMsgNoLookups=true -Xmx8192m -XX:+UseG1GC \
  -Xlog:gc*:${LOG_DIR}/fe.gc.log.$DATE:time \
  -XX:ErrorFile=${LOG_DIR}/hs_err_pid%p.log \
  -XX:+HeapDumpOnOutOfMemoryError \
  -XX:HeapDumpPath=${LOG_DIR}/heap_dump_oom.hprof \
  -Djava.security.policy=${STARROCKS_HOME}/conf/udf_security.policy \
  -Dcom.sun.management.jmxremote.authenticate=false \
  -Dcom.sun.management.jmxremote.ssl=false \
  -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:8113"
```

**3. マシン自体のメモリが限られている場合**、再起動してもOOMの問題が発生する可能性があります。マシンのメモリを拡張してから、JVMの設定を増やして再起動することを検討してください。

**4. ケースで過去に対応するPRが見つかった場合**、対応する修正バージョンへのアップグレードをお勧めします。

## ケース集

### ケース1：頻繁かつ大規模なメタデータ操作によるFEメモリ使用量の増大

**背景**：あるクライアントのビジネスでは、定期的なテーブルの作成とクリーンアップが必要であり、順次操作で1秒あたり数十のテーブルを作成し、同期操作でそれらを削除するため、プロセス全体で数十万のテーブルが作成されます。

**問題の現れ方**：ヒープメモリとマシンCPUが上昇し続け、クエリの応答が遅くなります。FEログを確認したところ、大量のテーブル作成および削除操作が見つかり、サービスへの負荷が急増しました。

**復旧方法**：テーブルの作成および削除ジョブを停止し、サービスを再起動して、通常の使用を再開します。

**解決策**：このビジネスシナリオでは、`DROP TABLE` を使用する場合、`FORCE` を追加する必要があります。そうしないと、メタデータが占有され続けます。同時に、テーブル作成の並行頻度を下げてください。

***

### ケース2：複雑な並行SQLがFEメモリの急増を引き起こす

**問題の症状**：ヒープメモリが突然増加し、スケジュールされたインポートタスクが大幅に遅延しました。QPSおよび接続数は通常の変動範囲内でした。監査ログおよびFEログを確認したところ、並行スケジュールSQLに非常に複雑なクエリが多数含まれており、クエリプランの解析中に大量のメモリが占有されていることが判明しました。

**復旧方法**：FEサービスのメモリを拡張します。マシンを停止してメモリを拡張し、JVM XMX設定を増やして、サービスを再起動します。

**解決策**：複雑なSQLを分割し、ジョブのピーク時間帯のメモリ使用量を監視して、80%未満に保つようにします。

***

### ケース3：MVのリフレッシュが頻繁すぎるため、リーダーFEのメモリがゆっくりと増加し、高い状態が続く

**背景**：ユーザーはMVリフレッシュタスクを持っており、多くのタスクが分または秒レベルのリフレッシュに設定されています。

**問題の症状**：FEリーダーのメモリが上昇し続け、クエリが遅延しています。FEログを確認すると、多くのスローロックが見つかり、占有されたリソースが解放されない原因となっています。

スローロックログの例：

```
2025-06-06 15:41:43.813+08:00 WARN (AutoStatistic|41)
[LockManager.logSlowLockTrace():423] LockManager detects slow lock :
{"owners":[{"id":13479085,"name":"starrocks-taskrun-pool-22075","type":"INTENTION_SHARED","heldFor":6935,"queryId":"ab976f93-42a9-11f0-98e6-fa163e3710f8","waitTime":0,"stack":["java.base@11.0.20.1/java.net.SocketInputStream.socketRead0(Native Method)","java.base@11.0.20.1/java.net.SocketInputStream.socketRead(SocketInputStream.java:115)","java.base@11.0.20.1/java.net.SocketInputStream.read(SocketInputStream.java:168)","java.base@11.0.20.1/java.net.SocketInputStream.read(SocketInputStream.java:140)","app//org.postgresql.core.VisibleBufferedInputStream.readMore(VisibleBufferedInputStream.java:161)","app//org.postgresql.core.VisibleBufferedInputStream.ensureBytes(VisibleBufferedInputStream.java:128)","app//org.postgresql.core.VisibleBufferedInputStream.ensureBytes(VisibleBufferedInputStream.java:113)","app//org.postgresql.core.VisibleBufferedInputStream.read(VisibleBufferedInputStream.java:73)","app//org.postgresql.core.PGStream.receiveChar(PGStream.java:453)","app//org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2120)","app//org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:356)","app//org.postgresql.jdbc.PgStatement.executeInternal(PgStatement.java:496)","app//org.postgresql.jdbc.PgStatement.execute(PgStatement.java:413)","app//org.postgresql.jdbc.PgStatement.executeWithFlags(PgStatement.java:333)","app//org.postgresql.jdbc.PgStatement.executeCachedSql(PgStatement.java:319)","app//org.postgresql.jdbc.PgStatement.executeWithFlags(PgStatement.java:295)","app//org.postgresql.jdbc.PgStatement.executeQuery(PgStatement.java:244)","app//org.postgresql.jdbc.PgDatabaseMetaData.getColumns(PgDatabaseMetaData.java:1584)","app//com.starrocks.connector.jdbc.PostgresSchemaResolver.getColumns(PostgresSchemaResolver.java:49)","app//com.starrocks.connector.jdbc.JDBCMetadata.lambda$getTable$1(JDBCMetadata.java:200)","app//com.starrocks.connector.jdbc.JDBCMetadata$$Lambda$1340/0x000014e17aa20960.apply(Unknown Source)","app//com.starrocks.connector.jdbc.JDBCMetaCache.get(JDBCMetaCache.java:73)","app//com.starrocks.connector.jdbc.JDBCMetadata.getTable(JDBCMetadata.java:197)","app//com.starrocks.connector.CatalogConnectorMetadata.getTable(CatalogConnectorMetadata.java:136)","app//com.starrocks.server.MetadataMgr.lambda$getTable$5(MetadataMgr.java:501)","app//com.starrocks.server.MetadataMgr$$Lambda$513/0x000014e2116f0cb0.apply(Unknown Source)","java.base@11.0.20.1/java.util.Optional.map(Optional.java:265)","app//com.starrocks.server.MetadataMgr.getTable(MetadataMgr.java:501)","app//com.starrocks.sql.analyzer.QueryAnalyzer.resolveTable(QueryAnalyzer.java:1391)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:482)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:420)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.resolveTableRef(QueryAnalyzer.java:420)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:363)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:907)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SubqueryRelation.accept(SubqueryRelation.java:66)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:370)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:907)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSubquery(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SubqueryRelation.accept(SubqueryRelation.java:66)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitJoin(QueryAnalyzer.java:739)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitJoin(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.JoinRelation.accept(JoinRelation.java:134)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:370)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitSelect(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.SelectRelation.accept(SelectRelation.java:232)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryRelation(QueryAnalyzer.java:303)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:293)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.visitQueryStatement(QueryAnalyzer.java:283)","app//com.starrocks.sql.ast.QueryStatement.accept(QueryStatement.java:70)","app//com.starrocks.sql.analyzer.QueryAnalyzer$Visitor.process(QueryAnalyzer.java:288)","app//com.starrocks.sql.analyzer.QueryAnalyzer.analyze(QueryAnalyzer.java:121)","app//com.starrocks.sql.analyzer.InsertAnalyzer.analyzeWithDeferredLock(InsertAnalyzer.java:74)","app//com.starrocks.sql.analyzer.InsertAnalyzer.analyze(InsertAnalyzer.java:62)","app//com.starrocks.sql.analyzer.Analyzer$AnalyzerVisitor.visitInsertStatement(Analyzer.java:398)","app//com.starrocks.sql.analyzer.Analyzer$AnalyzerVisitor.visitInsertStatement(Analyzer.java:176)","app//com.starrocks.sql.ast.InsertStmt.accept(InsertStmt.java:304)","app//com.starrocks.sql.ast.AstVisitor.visit(AstVisitor.java:80)","app//com.starrocks.sql.analyzer.Analyzer.analyze(Analyzer.java:173)","app//com.starrocks.sql.StatementPlanner.analyzeStatement(StatementPlanner.java:218)","app//com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:116)","app//com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:95)","app//com.starrocks.load.InsertOverwriteJobRunner.executeInsert(InsertOverwriteJobRunner.java:351)","app//com.starrocks.load.InsertOverwriteJobRunner.doLoad(InsertOverwriteJobRunner.java:171)","app//com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:151)","app//com.starrocks.load.InsertOverwriteJobRunner.transferTo(InsertOverwriteJobRunner.java:212)","app//com.starrocks.load.InsertOverwriteJobRunner.prepare(InsertOverwriteJobRunner.java:256)","app//com.starrocks.load.InsertOverwriteJobRunner.handle(InsertOverwriteJobRunner.java:148)","app//com.starrocks.load.InsertOverwriteJobRunner.run(InsertOverwriteJobRunner.java:136)","app//com.starrocks.load.InsertOverwriteJobMgr.executeJob(InsertOverwriteJobMgr.java:91)","app//com.starrocks.qe.StmtExecutor.handleInsertOverwrite(StmtExecutor.java:2152)","app//com.starrocks.qe.StmtExecutor.handleDMLStmt(StmtExecutor.java:2244)","app//com.starrocks.qe.StmtExecutor.handleDMLStmtWithProfile(StmtExecutor.java:2161)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.refreshMaterializedView(PartitionBasedMvRefreshProcessor.java:1271)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doRefreshMaterializedView(PartitionBasedMvRefreshProcessor.java:464)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doRefreshMaterializedViewWithRetry(PartitionBasedMvRefreshProcessor.java:373)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.doMvRefresh(PartitionBasedMvRefreshProcessor.java:332)","app//com.starrocks.scheduler.PartitionBasedMvRefreshProcessor.processTaskRun(PartitionBasedMvRefreshProcessor.java:200)","app//com.starrocks.scheduler.TaskRun.executeTaskRun(TaskRun.java:285)","app//com.starrocks.scheduler.TaskRunExecutor.lambda$executeTaskRun$0(TaskRunExecutor.java:60)","app//com.starrocks.scheduler.TaskRunExecutor$$Lambda$3311/0x000014db15a06cb0.get(Unknown Source)","java.base@11.0.20.1/java.util.concurrent.CompletableFuture$AsyncSupply.run(CompletableFuture.java:1700)","java.base@11.0.20.1/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)","java.base@11.0.20.1/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)","java.base@11.0.20.1/java.lang.Thread.run(Thread.java:829)"]}],"waiter":[{"id":13446845,"name":"thrift-server-pool-367701","type":"WRITE","waitTime":6895},{"id":5268046,"name":"lake-publish-task-8973","type":"INTENTION_EXCLUSIVE","waitTime":6895},{"id":5268447,"name":"lake-publish-task-9181","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":5267898,"name":"lake-publish-task-8911","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":5267726,"name":"lake-publish-task-8827","type":"INTENTION_EXCLUSIVE","waitTime":6874},{"id":13494208,"name":"thrift-server-pool-369089","type":"INTENTION_EXCLUSIVE","waitTime":6857},{"id":13492395,"name":"starrocks-taskrun-pool-22096","type":"INTENTION_EXCLUSIVE","waitTime":6854},{"id":41,"name":"AutoStatistic","type":"INTENTION_SHARED","waitTime":6854,"queryId":"ad195549-42a9-11f0-98e6-fa163e3710f8"},{"id":486,"name":"nioEventLoopGroup-6-33","type":"INTENTION_SHARED","waitTime":6852},{"id":5267924,"name":"lake-publish-task-8925","type":"INTENTION_SHARED","waitTime":6849},{"id":5271934,"name":"lake-publish-task-9265","type":"INTENTION_SHARED","waitTime":6849},{"id":5344442,"name":"lake-publish-task-9312","type":"INTENTION_EXCLUSIVE","waitTime":6786},{"id":13471238,"name":"thrift-server-pool-368401","type":"INTENTION_SHARED","waitTime":55},{"id":13494164,"name":"thrift-server-pool-369065","type":"INTENTION_SHARED","waitTime":55},{"id":13481141,"name":"thrift-server-pool-368676","type":"INTENTION_SHARED","waitTime":54}]}
```

**復旧方法**：リーダーのGC後、プライマリに切り替わり、自己回復します。

**解決策**：リフレッシュ頻度を緩和します。リアルタイムに強く依存しないシナリオを時間単位または日単位に調整し、大量の分レベルのMVが同時にトリガーされないようにします。スローロックが消えれば、問題は解決されます。

***

### ケース4：IcebergカタログのExternalテーブルへのクエリがFEメモリの増加を引き起こす

**背景**：ユーザーがIcebergテーブルに対してクエリジョブを実行します。

**問題の症状**：SQLを送信した後、FEリーダーのメモリが大幅に増加し、CPUも上昇し続けました。他のクエリおよびインポートタスクがエラーを報告しました。プロセスの状態を確認したところ、頻繁なGCがトリガーされ、サービスが異常な状態になっていることが判明しました。

**復旧方法**：リーダーノードを再起動し、対応するIcebergテーブルの削除ファイルを定期的に管理します。

***

### ケース5：Insertのメモリリーク問題がFEメモリの異常な増加を引き起こす

**背景**：ユーザーの操作中に、FEメモリが異常に増加し、プロセスが再起動し、リーダーが適切に切り替わらないことが判明しました。クライアントクラスターの主なタスクはインポートタスクでした。

**問題の症状**：トラブルシューティングのために、リーダーノードプロセスの `jmap` をエクスポートします：

```bash
jmap -histo <FEpid> > jmap.txt  # This operation will not trigger Full GC
```

エクスポートされたファイルで以下が見つかりました：

```bash
14: 521358 154321968 com.starrocks.load.loadv2.InsertLoadJob
```

`InsertLoadJob` インスタンスの数は52万件です。この数が1万件を超えると、メモリリークが発生していることを示します。

- [Issue](https://github.com/StarRocks/starrocks/issues/538100
- [修正PR](https://github.com/StarRocks/starrocks/pull/53809)

**影響を受けるバージョン**：

- 3.1.0 - 3.1.16
- 3.2.0 - 3.2.12
- 3.3.0 - 3.3.7

**修正済みバージョン**：3.1.17+、3.2.13+、3.3.8+

**復旧方法**：クラスターバージョンをアップグレードしてください。

***

### ケース6：async-profilerがJVMクラッシュを引き起こし、FEクラッシュにつながる

**問題の現象**：FEプロセスがクラッシュまたは再起動した。

**問題のトラブルシューティング**：プロセスが異常であり、ログディレクトリに `hs_err_pid$pid.log` ファイルが生成されている場合、FEのクラッシュはJVMクラッシュによって引き起こされている可能性があります。

```
--------------- S U M M A R Y ------------
...
--------------- T H R E A D ---------------
Current thread: JavaThread "tablet scheduler" ...

Native frames:
V  [libjvm.so] frame::entry_frame_is_first() const
V  [libjvm.so] forte_fill_call_trace_given_top(...)
V  [libjvm.so] AsyncGetCallTrace
C  [libasyncProfiler.so] Profiler::getJavaTraceAsync(...)
C  [libasyncProfiler.so] Profiler::recordSample(...)
C  [libasyncProfiler.so] PerfEvents::signalHandler(...)
```

クラッシュのブレークポイント `PerfEvents::signalHandler` は `async-profiler` によって引き起こされています。このツールはJavaコールスタックを取得しようとした際にJVM内でセグメンテーションフォルトを引き起こしました。エンタープライズ版では、このツールはデフォルトでデプロイされ、定期的にプロセス情報を取得します。

**処理方法**：`async-profiler` のCPU情報収集を停止します：

```sql
ADMIN SET FRONTEND CONFIG ("proc_profile_cpu_enable" = "false");
```

`fe.conf` に以下も追加してください：

```properties
proc_profile_cpu_enable = false
```

***

### ケース7：JVMヒープ設定の不足によるOOM

通常の状況では、FEが3台ある場合、リーダーノードがメモリ不足でクラッシュしても、残りの2台のFEが新しいリーダーを選出してサービスを提供できます。リーダーがクラッシュした後に2台のフォロワーノードが正常に通信できない場合、リーダーに昇格しようとしているFEもメタデータレプリカ不足により終了し、2台のFEがクラッシュする可能性があります。

**問題のトラブルシューティング**：

FEリーダーのログに以下が表示されます：

```
java.lang.OutOfMemoryError: Java heap space
2025-08-09 08:22:30.926-06:00 WARN (thrift-server-pool-9443633|65019425)
[TIOStreamTransport.close():153] Error closing output stream.
java.net.SocketException: Socket closed
    at java.net.SocketOutputStream.socketWrite(SocketOutputStream.java:113) ~[?:?]
    at java.net.SocketOutputStream.write(SocketOutputStream.java:150) ~[?:?]
    at java.io.BufferedOutputStream.flushBuffer(BufferedOutputStream.java:81) ~[?:?]
    at java.io.BufferedOutputStream.flush(BufferedOutputStream.java:142) ~[?:?]
    at java.io.FilterOutputStream.close(FilterOutputStream.java:182) ~[?:?]
    at org.apache.thrift.transport.TIOStreamTransport.close(TIOStreamTransport.java:151) ~[libthrift-0.20.0.jar:0.20.0]
    at org.apache.thrift.transport.TSocket.close(TSocket.java:238) ~[libthrift-0.20.0.jar:0.20.0]
    at com.starrocks.common.SRTThreadPoolServer$WorkerProcess.run(SRTThreadPoolServer.java:326) ~[starrocks-fe.jar:?]
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
    at java.lang.Thread.run(Thread.java:829) ~[?:?]
2025-08-09 08:22:30.927-06:00 ERROR (thrift-server-accept|144)
[SRTThreadPoolServer.execute():221] ExecutorService threw error:
java.lang.OutOfMemoryError: Java heap space
java.lang.OutOfMemoryError: Java heap space
2025-08-09 08:22:30.931-06:00 WARN (starrocks-mysql-nio-pool-42252|65019486)
[StmtExecutor.execute():595] New planner error: ...
com.starrocks.sql.common.StarRocksPlannerException: StarRocks planner use long time 3000 ms in memo phase,
This probably because 1. FE Full GC, 2. Hive external table fetch metadata took a long time, 3. The SQL is
very complex. You could 1. adjust FE JVM config, 2. try query again, 3. enlarge new_planner_optimize_timeout
session variable
    at com.starrocks.sql.optimizer.task.SeriallyTaskScheduler.executeTasks(SeriallyTaskScheduler.java:50) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.memoOptimize(Optimizer.java:900) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.optimizeByCost(Optimizer.java:272) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.optimizer.Optimizer.optimize(Optimizer.java:196) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.createQueryPlanWithReTry(StatementPlanner.java:348) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:138) ~[starrocks-fe.jar:?]
    at com.starrocks.sql.StatementPlanner.plan(StatementPlanner.java:95) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.StmtExecutor.execute(StmtExecutor.java:580) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.handleQuery(ConnectProcessor.java:389) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.dispatch(ConnectProcessor.java:598) ~[starrocks-fe.jar:?]
    at com.starrocks.qe.ConnectProcessor.processOnce(ConnectProcessor.java:936) ~[starrocks-fe.jar:?]
    at com.starrocks.mysql.nio.ReadListener.lambda$handleEvent$0(ReadListener.java:69) ~[starrocks-fe.jar:?]
    at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
    at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
    at java.lang.Thread.run(Thread.java:829) ~[?:?]
2025-08-09 08:22:30.932-06:00 INFO (TaskCleaner|111)
[TaskManager.dropTasks():392] drop tasks:[]
2025-08-09 08:22:35.431-06:00 INFO (main|1)
[StarRocksFE.start():123] StarRocks FE starting, version: 3.3.14-ee-5b29ea9
```

リーダーに昇格しようとしているFEが失敗します：

```
    at com.starrocks.common.util.Daemon.run(Daemon.java:109) ~[starrocks-fe.jar:?]
2025-08-09 08:16:07.844-06:00 ERROR (stateChangeExecutor|86)
[GlobalStateMgr.transferToLeader():1333] failed to init journal after transfer to leader! will exit
com.starrocks.journal.JournalException: catch exception after retried 3 times
    at com.starrocks.journal.bdbje.BDBJEJournal.open(BDBJEJournal.java:222) ~[starrocks-fe.jar:?]
    at com.starrocks.server.GlobalStateMgr.transferToLeader(GlobalStateMgr.java:1323) ~[starrocks-fe.jar:?]
    at com.starrocks.server.GlobalStateMgr$1.transferToLeader(GlobalStateMgr.java:795) ~[starrocks-fe.jar:?]
    at com.starrocks.ha.StateChangeExecutor.runOneCycle(StateChangeExecutor.java:125) ~[starrocks-fe.jar:?]
    at com.starrocks.common.util.Daemon.run(Daemon.java:109) ~[starrocks-fe.jar:?]
Caused by: com.sleepycat.je.rep.InsufficientReplicasException: (JE 18.3.20) Commit policy:
SIMPLE_MAJORITY required 1 replica. But none were active with this master.
    at com.sleepycat.je.rep.impl.node.DurabilityQuorum.ensureReplicasForCommit(DurabilityQuorum.java:116) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.impl.RepImpl.txnBeginHook(RepImpl.java:1171) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.txnBeginHook(MasterTxn.java:195) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.initTxn(Txn.java:384) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.<init>(Txn.java:288) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.<init>(Txn.java:267) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.<init>(MasterTxn.java:146) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn$1.create(MasterTxn.java:117) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.txn.MasterTxn.create(MasterTxn.java:435) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.rep.impl.RepImpl.createRepUserTxn(RepImpl.java:1145) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.Txn.createAutoTxn(Txn.java:334) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.txn.LockerFactory.getWritableLocker(LockerFactory.java:79) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.Environment.setupDatabase(Environment.java:816) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.sleepycat.je.Environment.openDatabase(Environment.java:668) ~[starrocks-bdb-je-18.3.20.jar:?]
    at com.starrocks.journal.bdbje.BDBEnvironment.openDatabase(BDBEnvironment.java:446) ~[starrocks-fe.jar:?]
    at com.starrocks.journal.bdbje.BDBJEJournal.open(BDBJEJournal.java:213) ~[starrocks-fe.jar:?]
    ... 4 more
2025-08-09 08:16:07.845-06:00 INFO (Thread-69|145)
[StarRocksFE.lambda$addShutdownHook$1():374] start to execute shutdown hook
2025-08-09 08:16:07.852-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 69224
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 31128
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 64464
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 119040
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 83760
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 69080
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 83760
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:show frontends;, QueryFEAllocatedMemory: 84072
2025-08-09 08:16:07.853-06:00 WARN (Thread-70|2103846)
[ConnectScheduler.lambda$printAllRunningQuery$4():339] FE ShutDown! Running Query:SET NAMES utf8, QueryFEAllocatedMemory: 10048
```

**処理方法**：FEのJVMメモリ設定を増やしてください。本番環境では最低16GBを推奨し、その後はメタデータの増加に応じて調整してください。復旧については、[メタデータの復旧](../administration/Meta_recovery.md)ドキュメントのステップ8と9を参照してください。

***

### ケース8：FEの応答遅延によるインポートタスクの処理時間増加

**問題の現象**：エージェントはFEと通信する際にFEが適時に応答しない状況に遭遇すると `jstack` を出力します。2台のフォロワーFEのログには「notify new FE type」というメッセージが出力されています。エージェントログには大量の `jstack` 収集レコードがあります。

`jstack` の収集がFEの無応答状態の継続時間を悪化させました。

**処理方法**：`jstack` の収集をオフにしてください。バージョン3.3+ではすでにデフォルトでオフになっています。クラスター設定から `jstack` コマンドを削除できます。

***

### ケース9：FEメモリの緩やかな増加（バージョン3.3.13 - 3.3.18）

**バージョン範囲**：この問題はバージョン3.3.13から3.3.18の間で発生します。

**問題の現象**：FEヒープメモリが継続的かつ緩やかに増加している場合、この問題が原因である可能性があります。

`jmap` を取得して確認すると、`com.starrocks.catalog.Replica` が過剰なメモリを占有していることがわかります。`DROP`、`SWAP`、および `INSERT OVERWRITE` を大量に含むすべての操作がこの問題を引き起こす可能性があります。

この問題は、バージョン3.3.13でタブレット削除パスの最適化が行われた際にメモリリークが導入されたことが原因であり、3.3.18で修正されました。

- 3.3.13での最適化PR：[BugFix] Fix recycle bin missing to delete lake mv's expired partitions after mv refreshed
- 修正PRは[3.3.18](https://github.com/StarRocks/starrocks/pull/61582)

**処理方法**:

- 一時的な回避策：FEを再起動する
- 根本的な解決策：修正済みバージョンにアップグレードする

**修正済みバージョン**：3.3.18、3.4.7、3.5.4

***

## 付録：ツール使用コマンド

### jstat — JVM GCステータスの監視

```bash
jstat -gcutil <pid> 1000 10
```

FEプロセスのGC使用率を1秒ごとに1回出力し、連続して10回出力します。

**主な指標：**

| 指標 | 意味 |
|-----------|-----------------------------------------------|
| S0, S1 | Survivorスペース使用率（Young Generation） |
| E | Edenスペース使用率（Young Generation） |
| O | Old Generation使用率 |
| YGC, YGCT | Young GC回数と時間 |
| FGC, FGCT | Full GC回数と時間 |

**目的**：GCが頻繁かどうか、Old Generationが満杯に近づいているかどうか、Full GCが発生しているかどうかを素早く判断する。

### jmap — JVMメモリオブジェクト分布の確認

**方法1：現在のヒープ内のオブジェクトを確認する（ガベージオブジェクトを含む）**

```bash
jmap -histo <pid> | head -n 30
```

すべてのオブジェクトタイプのインスタンス数と占有サイズをカウントします。これを使用して、`byte[]`、`String`、`HashMap`などのヒープ内の大きなオブジェクトを確認します。

**方法2：強制GC後に生存オブジェクトを確認する**

```bash
jmap -histo:live <pid> | head -n 30
```

Full GCをトリガーし、GC後も生存しているオブジェクトのみを表示します。リークしたオブジェクトやキャッシュが解放されないなどの問題のトラブルシューティングに使用します。
