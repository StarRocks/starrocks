---
displayed_sidebar: docs
---

# デプロイメント

このトピックでは、デプロイメントに関するよくある質問への回答を提供します。

## `fe.conf` ファイルの `priority_networks` パラメータで固定 IP アドレスをバインドするにはどうすればよいですか？

### 問題の説明

例えば、2つの IP アドレスがある場合: 192.168.108.23 と 192.168.108.43。IP アドレスを次のように指定するかもしれません:

- アドレスを 192.168.108.23/24 と指定すると、StarRocks はそれを 192.168.108.43 と認識します。
- アドレスを 192.168.108.23/32 と指定すると、StarRocks はそれを 127.0.0.1 と認識します。

### 解決策

この問題を解決する方法は次の2つです:

- IP アドレスの末尾に「32」を追加しないか、「32」を「28」に変更します。
- または、StarRocks 2.1 以降にアップグレードすることもできます。

## インストール後にバックエンド (BE) を起動すると「StarRocks BE http サービスが正しく起動しませんでした、終了します」というエラーが発生するのはなぜですか？

BE をインストールすると、システムは起動エラーを報告します: StarRocks BE http サービスが正しく起動しませんでした、終了します。

このエラーは、BE のウェブサービスポートが占有されているために発生します。`be.conf` ファイル内のポートを変更し、BE を再起動してみてください。

## ERROR 1064 (HY000): Could not initialize class com.starrocks.rpc.BackendServiceProxy というエラーが発生した場合はどうすればよいですか？

このエラーは、Java Runtime Environment (JRE) でプログラムを実行するときに発生します。この問題を解決するには、JRE を Java Development Kit (JDK) に置き換えてください。Oracle の JDK 17 以降を使用することをお勧めします。

## FE と BE の設定項目を変更して、クラスタを再起動せずに有効にすることはできますか？

はい。FE と BE の設定項目を変更するには、次の手順を実行します:

- FE: FE の変更は次のいずれかの方法で行うことができます:
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

- BE: BE の変更は次の方法で行うことができます:

```plaintext
curl -XPOST -u username:password \
http://<ip>:<be_http_port>/api/update_config?key=value
```

> 注: ユーザーがリモートでログインする権限を持っていることを確認してください。そうでない場合は、次の方法でユーザーに権限を付与できます:

```plaintext
CREATE USER 'test'@'%' IDENTIFIED BY '123456';

GRANT SELECT ON . TO 'test'@'%';
```

## クラスタの再起動中に FE を起動すると「Fe type:unknown ,is ready :false.」というエラーが発生するのはなぜですか？

Leader FE が実行中かどうかを確認してください。実行中でない場合は、クラスタ内の FE ノードを一つずつ再起動してください。

## クラスタをデプロイすると「failed to get service info err.」というエラーが発生するのはなぜですか？

OpenSSH Daemon (sshd) が有効になっているか確認してください。有効でない場合は、`/etc/init.d/sshd status` コマンドを実行して有効にしてください。

## BE を起動すると「Fail to get master client from `cache. ``host= port=0 code=THRIFT_RPC_ERROR`」というエラーが発生するのはなぜですか？

`netstat -anp |grep port` コマンドを実行して、`be.conf` ファイル内のポートが占有されているかどうかを確認してください。占有されている場合は、占有されていないポートに置き換えてから BE を再起動してください。

## 新しくデプロイされた FE ノードが正常に動作しているのに、StarRocks Manager の診断ページの FE ノードログに「Search log failed.」と表示されるのはなぜですか？

デフォルトでは、StarRocks Manager は新しくデプロイされた FE のパス設定を 30 秒以内に取得します。このエラーは、FE が遅く起動するか、他の理由で 30 秒以内に応答しない場合に発生します。Manager Web のログを次のパスで確認してください:

`/starrocks-manager-xxx/center/log/webcenter/log/web/``drms.INFO`(パスはカスタマイズ可能です)。ログに「Failed to update FE configurations」というメッセージが表示されているか確認してください。表示されている場合は、対応する FE を再起動して新しいパス設定を取得してください。

## FE を起動すると「exceeds max permissable delta:5000ms.」というエラーが発生するのはなぜですか？

このエラーは、2台のマシン間の時間差が5秒以上ある場合に発生します。この問題を解決するには、これら2台のマシンの時間を合わせてください。

## データストレージ用に BE に複数のディスクがある場合、`storage_root_path` パラメータをどのように設定しますか？

`be.conf` ファイルで `storage_root_path` パラメータを設定し、このパラメータの値を `;` で区切ります。例: `storage_root_path=/the/path/to/storage1;/the/path/to/storage2;/the/path/to/storage3;`

## クラスタに FE を追加した後に「invalid cluster id: 209721925.」というエラーが発生するのはなぜですか？

クラスタを初めて起動する際に、この FE に `--helper` オプションを追加しない場合、2台のマシン間でメタデータが不一致となり、このエラーが発生します。この問題を解決するには、メタディレクトリのすべてのメタデータをクリアし、`--helper` オプションを使用して FE を追加する必要があります。

## FE が実行中でログに `transfer: follower` と表示されるときに Alive が `false` になるのはなぜですか？

この問題は、Java Virtual Machine (JVM) のメモリの半分以上が使用され、チェックポイントがマークされていない場合に発生します。通常、システムが 50,000 件のログを蓄積した後にチェックポイントがマークされます。各 FE の JVM パラメータを変更し、これらの FEs が重くロードされていないときに再起動することをお勧めします。

クエリエラー: "could not initialize class com.starrocks.rpc.BackendServiceProxy"。この問題を解決するにはどうすればよいですか？

- 環境変数 `$JAVA_HOME` が正しい JDK パスを指していることを確認してください。
- すべてのノードが同一のJDKバージョンを使用していることを確認してください。すべてのノードは同一のJDKバージョンを使用する必要があります。

## StarRocks をインストールするための MySQL バージョン要件は何ですか？

StarRocks に接続するには MySQL 5.7 以降を推奨します。

## FE と BE を同じマシンにデプロイした場合、どのように分離できますか？

最初に BE ノードをスケールアウトすることをお勧めします。クラスタのバランスが完了した後、元の BE ノードをスケールインできます。

## FE が「Replica exceeds max permissible delta:5000ms」というエラーで起動に失敗した場合はどうすればよいですか？

FE ノード間のクロックが同期されていません。FE ノード間の時間差は5秒未満である必要があります。

## 本番環境に5台の物理マシンがある場合、推奨される StarRocks のデプロイメントは何ですか？

推奨されるデプロイメントは、3つの FE ノードと5つの BE ノードです。

## StarRocks をインストールする際に root ユーザーを使用すべきですか？

root ユーザーは権限が過剰であるため、使用することは推奨されません。StarRocks をインストールするための専用ユーザーを作成してください。

## MySQL クライアントを任意のマシンにインストールできますか？

はい。これは単なるクライアントツールであり、StarRocks と同じマシンで実行する必要はありません。MySQL クライアントがクラスタにアクセスできることを確認してください。

## BE ノード上の tablet の数に制限はありますか？たとえば、64 GB RAM と 16 コアのサーバーの場合、tablet 数の合理的な範囲はどのくらいですか？

tablet 数に厳密な制限はありません。ただし、tablet サイズについては、各 tablet を約 1 GB に保つことをお勧めします。適切なパーティショニングとバケッティング戦略がクエリパフォーマンスを向上させます。tablet サイズの計画は重要です。

## BE を起動すると「error while loading shared libraries: libjvm.so: cannot open shared object file: No such file or directory」というエラーが表示され、手動で `lib/starrocks_be` ディレクトリを作成すると、許可が拒否されるエラーが発生します。どうすればよいですか？

JDK のインストールに問題があります。JDK 環境を再インストールして適切に構成してください。

## StarRocks は AMD AVX2 での実行をサポートしていますか？Intel と AMD サーバーを混在させると問題が発生しますか？

StarRocks は AMD で実行できます。Intel と AMD サーバーを混在させることは推奨されません。ハードウェアの異質性が問題を引き起こす可能性があるためです。AMD への移行前に StarRocks を十分にテストすることをお勧めします。