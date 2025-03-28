---
displayed_sidebar: docs
sidebar_position: 60
---

# ロードバランシング

複数の FE ノードをデプロイする際、ユーザーは FEs の上にロードバランシング層をデプロイして高可用性を実現できます。

以下は高可用性のオプションです。

## コードアプローチ

1つの方法は、アプリケーション層でコードを実装してリトライとロードバランシングを行うことです。例えば、接続が切れた場合、他の接続で自動的にリトライします。この方法では、ユーザーが複数の FE ノードアドレスを設定する必要があります。

## JDBC コネクタ

JDBC コネクタは自動リトライをサポートしています。

~~~sql
jdbc:mysql:loadbalance://[host1][:port],[host2][:port][,[host3][:port]]...[/[database]][?propertyName1=propertyValue1[&propertyName2=propertyValue2]...]
~~~

## ProxySQL

ProxySQL は、読み書きの分離、クエリルーティング、SQL キャッシング、動的ロード構成、フェイルオーバー、SQL フィルタリングをサポートする MySQL プロキシ層です。

StarRocks FE は接続とクエリリクエストを受け取る役割を担っており、水平スケーラブルで高可用性があります。しかし、FE は自動ロードバランシングを実現するために、その上にプロキシ層を設定する必要があります。設定手順は以下の通りです。

### 1. 関連する依存関係をインストール

~~~shell
yum install -y gnutls perl-DBD-MySQL perl-DBI perl-devel
~~~

### 2. インストールパッケージをダウンロード

~~~shell
wget https://github.com/sysown/proxysql/releases/download/v2.0.14/proxysql-2.0.14-1-centos7.x86_64.rpm
~~~

### 3. 現在のディレクトリに解凍

~~~shell
rpm2cpio proxysql-2.0.14-1-centos7.x86_64.rpm | cpio -ivdm
~~~

### 4. 設定ファイルを修正

~~~shell
vim ./etc/proxysql.cnf 
~~~

ユーザーがアクセス権を持つディレクトリに指示します（絶対パス）:

~~~vim
datadir="/var/lib/proxysql"
errorlog="/var/lib/proxysql/proxysql.log"
~~~

### 5. 起動

~~~shell
./usr/bin/proxysql -c ./etc/proxysql.cnf --no-monitor
~~~

### 6. ログイン

~~~shell
mysql -u admin -padmin -h 127.0.0.1 -P6032
~~~

### 7. グローバルログを設定

~~~shell
SET mysql-eventslog_filename='proxysql_queries.log';
SET mysql-eventslog_default_log=1;
SET mysql-eventslog_format=2;
LOAD MYSQL VARIABLES TO RUNTIME;
SAVE MYSQL VARIABLES TO DISK;
~~~

### 8. リーダーノードに挿入

~~~sql
insert into mysql_servers(hostgroup_id, hostname, port) values(1, '172.26.92.139', 8533);
~~~

### 9. オブザーバーノードに挿入

~~~sql
insert into mysql_servers(hostgroup_id, hostname, port) values(2, '172.26.34.139', 9931);
insert into mysql_servers(hostgroup_id, hostname, port) values(2, '172.26.34.140', 9931);
~~~

### 10. 設定をロード

~~~sql
load mysql servers to runtime;
save mysql servers to disk;
~~~

### 11. ユーザー名とパスワードを設定

~~~sql
insert into mysql_users(username, password, active, default_hostgroup, backend, frontend) values('root', '*94BDCEBE19083CE2A1F959FD02F964C7AF4CFC29', 1, 1, 1, 1);
~~~

### 12. 設定をロード

~~~sql
load mysql users to runtime; 
save mysql users to disk;
~~~

### 13. プロキシルールに書き込み

~~~sql
insert into mysql_query_rules(rule_id, active, match_digest, destination_hostgroup, mirror_hostgroup, apply) values(1, 1, '.', 1, 2, 1);
~~~

### 14. 設定をロード

~~~sql
load mysql query rules to runtime; 
save mysql query rules to disk;
~~~