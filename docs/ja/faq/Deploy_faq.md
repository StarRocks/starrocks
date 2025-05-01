---
displayed_sidebar: docs
---

# デプロイ

このトピックでは、デプロイに関するよくある質問への回答を提供します。

## `fe.conf` ファイルの `priority_networks` パラメータで固定 IP アドレスをバインドするにはどうすればよいですか？

### 問題の説明

例えば、2つの IP アドレスがあるとします: 192.168.108.23 と 192.168.108.43。以下のように IP アドレスを指定することがあります。

- アドレスを 192.168.108.23/24 と指定すると、StarRocks はそれを 192.168.108.43 と認識します。
- アドレスを 192.168.108.23/32 と指定すると、StarRocks はそれを 127.0.0.1 と認識します。

### 解決策

この問題を解決する方法は次の2つです。

- IP アドレスの末尾に「32」を追加しないか、「32」を「28」に変更します。
- または、StarRocks 2.1 以降にアップグレードすることもできます。

## インストール後にバックエンド (BE) を起動すると「StarRocks BE http service did not start correctly, exiting」というエラーが発生するのはなぜですか？

BE をインストールすると、システムが起動エラーを報告します: StarRocks BE http service did not start correctly, exiting。

このエラーは、BE のウェブサービスポートが占有されているために発生します。`be.conf` ファイルのポートを変更し、BE を再起動してみてください。

## ERROR 1064 (HY000): Could not initialize class com.starrocks.rpc.BackendServiceProxy というエラーが発生した場合はどうすればよいですか？

このエラーは、Java Runtime Environment (JRE) でプログラムを実行する際に発生します。この問題を解決するには、JRE を Java Development Kit (JDK) に置き換えてください。Oracle の JDK 1.8 以降を使用することをお勧めします。

## FE と BE の設定項目を変更し、クラスタを再起動せずに反映させることはできますか？

はい。[FE と BE のパラメータ](../administration/management/FE_configuration.md) の設定を変更するには、次の手順を実行します。

- FE
  - SQL

  ```plaintext
  ADMIN SET FRONTEND CONFIG ("key" = "value");
  ```

  例:

  ```plaintext
  ADMIN SET FRONTEND CONFIG ("enable_statistic_collect" = "false");
  ```

  - シェル

  ```plaintext
  curl --location-trusted -u username:password \
  http://<ip>:<fe_http_port/api/_set_config?key=value>
  ```

  例:

  ```plaintext
  curl --location-trusted -u <username>:<password> \
  http://192.168.110.101:8030/api/_set_config?enable_statistic_collect=true
  ```

- BE

```plaintext
curl -XPOST -u username:password \
http://<ip>:<be_http_port>/api/update_config?key=value
```

> 注意: ユーザーがリモートでログインする権限を持っていることを確認してください。持っていない場合は、次の方法でユーザーに権限を付与できます。

```plaintext
CREATE USER 'test'@'%' IDENTIFIED BY '123456';

GRANT SELECT ON . TO 'test'@'%';
```

## BE のディスク容量を拡張した後に「Failed to get scan range, no queryable replica found in tablet:xxxxx」というエラーが発生した場合はどうすればよいですか？

### 問題の説明

このエラーは、主キーテーブルへのデータロード中に発生することがあります。データロード中に、宛先の BE にロードされたデータのための十分なディスク容量がなく、BE がクラッシュします。その後、新しいディスクが追加されてディスク容量が拡張されます。しかし、主キーテーブルはディスク容量の再バランスをサポートしておらず、データを他のディスクにオフロードすることができません。

### 解決策

このバグ（主キーテーブルが BE のディスク容量の再バランスをサポートしていない）へのパッチはまだ開発中です。現在、この問題を解決する方法は次の2つです。

- ディスク間でデータを手動で分配します。例えば、使用量の多いディスクから空き容量の多いディスクにディレクトリをコピーします。
- これらのディスク上のデータが重要でない場合は、ディスクを削除し、ディスクパスを変更することをお勧めします。このエラーが続く場合は、[TRUNCATE TABLE](../sql-reference/sql-statements/table_bucket_part_index/TRUNCATE_TABLE.md) を使用してテーブル内のデータをクリアし、スペースを解放します。

## クラスタの再起動中に FE を起動すると「Fe type:unknown ,is ready :false.」というエラーが発生するのはなぜですか？

Leader FE が実行中かどうかを確認してください。実行されていない場合は、クラスタ内の FE ノードを一つずつ再起動してください。

## クラスタをデプロイする際に「failed to get service info err.」というエラーが発生するのはなぜですか？

OpenSSH Daemon (sshd) が有効になっているか確認してください。有効でない場合は、`/etc/init.d/sshd status` コマンドを実行して有効にしてください。

## BE を起動すると「Fail to get master client from `cache. ``host= port=0 code=THRIFT_RPC_ERROR`」というエラーが発生するのはなぜですか？

`netstat -anp |grep port` コマンドを実行して、`be.conf` ファイルのポートが占有されているかどうかを確認してください。占有されている場合は、占有されていないポートに置き換えてから BE を再起動してください。

## 新しくデプロイされた FE ノードが正常に動作しているのに、StarRocks Manager の診断ページで「Search log failed.」と表示されるのはなぜですか？

デフォルトでは、StarRocks Manager は新しくデプロイされた FE のパス設定を 30 秒以内に取得します。このエラーは、FE が遅れて起動したり、他の理由で 30 秒以内に応答しない場合に発生します。Manager Web のログを次のパスで確認してください。

`/starrocks-manager-xxx/center/log/webcenter/log/web/``drms.INFO`（パスはカスタマイズ可能です）。ログに「Failed to update FE configurations」というメッセージが表示されているか確認してください。表示されている場合は、対応する FE を再起動して新しいパス設定を取得してください。

## FE を起動すると「exceeds max permissable delta:5000ms.」というエラーが発生するのはなぜですか？

このエラーは、2台のマシン間の時間差が5秒以上ある場合に発生します。この問題を解決するには、これら2台のマシンの時間を合わせてください。

## データストレージ用に複数のディスクがある場合、BE の `storage_root_path` パラメータをどのように設定しますか？

`be.conf` ファイルで `storage_root_path` パラメータを設定し、このパラメータの値を `;` で区切ります。例: `storage_root_path=/the/path/to/storage1;/the/path/to/storage2;/the/path/to/storage3;`

## FE をクラスタに追加した後に「invalid cluster id: 209721925.」というエラーが発生するのはなぜですか？

クラスタを初めて起動する際に、この FE に `--helper` オプションを追加しないと、2台のマシン間でメタデータが不一致になり、このエラーが発生します。この問題を解決するには、メタディレクトリのすべてのメタデータをクリアし、`--helper` オプションを使用して FE を追加する必要があります。

## FE が実行中でログに「transfer: follower」と表示されるとき、Alive が `false` になるのはなぜですか？

この問題は、Java Virtual Machine (JVM) のメモリの半分以上が使用され、チェックポイントがマークされていない場合に発生します。通常、システムが 50,000 件のログを蓄積した後にチェックポイントがマークされます。各 FE の JVM パラメータを変更し、負荷が高くないときにこれらの FE を再起動することをお勧めします。