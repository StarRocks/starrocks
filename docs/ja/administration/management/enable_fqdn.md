---
displayed_sidebar: docs
---

# FQDN アクセスを有効にする

このトピックでは、完全修飾ドメイン名 (FQDN) を使用してクラスタへのアクセスを有効にする方法について説明します。FQDN は、インターネットを介してアクセス可能な特定のエンティティの**完全なドメイン名**です。FQDN は、ホスト名とドメイン名の 2 つの部分で構成されています。

バージョン 2.4 以前では、StarRocks は IP アドレスを介した FEs および BEs へのアクセスのみをサポートしていました。FQDN を使用してクラスタにノードを追加しても、最終的には IP アドレスに変換されます。これにより、特定のノードの IP アドレスを変更すると、StarRocks クラスタ内のノードへのアクセスが失敗する可能性があるため、DBA にとって大きな不便が生じます。バージョン 2.4 では、StarRocks は各ノードをその IP アドレスから切り離しました。これにより、StarRocks 内のノードを FQDN のみで管理できるようになりました。

## 前提条件

StarRocks クラスタで FQDN アクセスを有効にするには、次の要件を満たしていることを確認してください。

- クラスタ内の各マシンにはホスト名が必要です。

- 各マシンの **/etc/hosts** ファイルに、クラスタ内の他のマシンの対応する IP アドレスと FQDN を指定する必要があります。

- **/etc/hosts** ファイル内の IP アドレスは一意でなければなりません。

## FQDN アクセスを使用して新しいクラスタをセットアップする

デフォルトでは、新しいクラスタ内の FE ノードは IP アドレスアクセスを介して開始されます。FQDN アクセスを使用して新しいクラスタを開始するには、**クラスタを初めて開始する際に**次のコマンドを実行して FE ノードを開始する必要があります。

```Shell
./bin/start_fe.sh --host_type FQDN --daemon
```

`--host_type` プロパティは、ノードを開始するために使用されるアクセス方法を指定します。有効な値には `FQDN` と `IP` が含まれます。このプロパティは、ノードを初めて開始する際に 1 回だけ指定する必要があります。

各 BE ノードは、FE メタデータで定義された `BE Address` で自分自身を識別します。したがって、BE ノードを開始する際に `--host_type` を指定する必要はありません。`BE Address` が FQDN で BE ノードを定義している場合、BE ノードはこの FQDN で自分自身を識別します。

## 既存のクラスタで FQDN アクセスを有効にする

IP アドレスを介して以前に開始された既存のクラスタで FQDN アクセスを有効にするには、まず StarRocks をバージョン 2.4.0 以降に**アップグレード**する必要があります。

### FE ノードの FQDN アクセスを有効にする

Leader FE ノードの FQDN アクセスを有効にする前に、すべての非 Leader Follower FE ノードの FQDN アクセスを有効にする必要があります。

> **注意**
>
> FE ノードの FQDN アクセスを有効にする前に、クラスタに少なくとも 3 つの Follower FE ノードがあることを確認してください。

#### 非 Leader Follower FE ノードの FQDN アクセスを有効にする

1. FE ノードのデプロイメントディレクトリに移動し、次のコマンドを実行して FE ノードを停止します。

    ```Shell
    ./bin/stop_fe.sh
    ```

2. MySQL クライアントを使用して次のステートメントを実行し、停止した FE ノードの `Alive` ステータスを確認します。`Alive` ステータスが `false` になるまで待ちます。

    ```SQL
    SHOW PROC '/frontends'\G
    ```

3. 次のステートメントを実行して、IP アドレスを FQDN に置き換えます。

    ```SQL
    ALTER SYSTEM MODIFY FRONTEND HOST "<fe_ip>" TO "<fe_hostname>";
    ```

4. 次のコマンドを実行して、FQDN アクセスで FE ノードを開始します。

    ```Shell
    ./bin/start_fe.sh --host_type FQDN --daemon
    ```

    `--host_type` プロパティは、ノードを開始するために使用されるアクセス方法を指定します。有効な値には `FQDN` と `IP` が含まれます。このプロパティは、ノードを変更した後に再起動する際に 1 回だけ指定する必要があります。

5. FE ノードの `Alive` ステータスを確認します。`Alive` ステータスが `true` になるまで待ちます。

    ```SQL
    SHOW PROC '/frontends'\G
    ```

6. 現在の FE ノードの `Alive` ステータスが `true` の場合、上記の手順を繰り返して、他の非 Leader Follower FE ノードの FQDN アクセスを順次有効にします。

#### Leader FE ノードの FQDN アクセスを有効にする

すべての非 Leader FE ノードが正常に変更され、再起動された後、Leader FE ノードの FQDN アクセスを有効にすることができます。

> **注意**
>
> Leader FE ノードが FQDN アクセスで有効になる前に、クラスタにノードを追加するために使用される FQDN は、対応する IP アドレスに変換され続けます。FQDN アクセスが有効な Leader FE ノードがクラスタのために選出された後、FQDN は IP アドレスに変換されません。

1. Leader FE ノードのデプロイメントディレクトリに移動し、次のコマンドを実行して Leader FE ノードを停止します。

    ```Shell
    ./bin/stop_fe.sh
    ```

2. MySQL クライアントを使用して次のステートメントを実行し、新しい Leader FE ノードがクラスタのために選出されたかどうかを確認します。

    ```SQL
    SHOW PROC '/frontends'\G
    ```

    ステータスが `Alive` で `isMaster` が `true` の FE ノードは、実行中の Leader FE です。

3. 次のステートメントを実行して、IP アドレスを FQDN に置き換えます。

    ```SQL
    ALTER SYSTEM MODIFY FRONTEND HOST "<fe_ip>" TO "<fe_hostname>";
    ```

4. 次のコマンドを実行して、FQDN アクセスで FE ノードを開始します。

    ```Shell
    ./bin/start_fe.sh --host_type FQDN --daemon
    ```

    `--host_type` プロパティは、ノードを開始するために使用されるアクセス方法を指定します。有効な値には `FQDN` と `IP` が含まれます。このプロパティは、ノードを変更した後に再起動する際に 1 回だけ指定する必要があります。

5. FE ノードの `Alive` ステータスを確認します。

    ```Plain
    SHOW PROC '/frontends'\G
    ```

  `Alive` ステータスが `true` になった場合、FE ノードは正常に変更され、クラスタに Follower FE ノードとして追加されます。

### BE ノードの FQDN アクセスを有効にする

MySQL クライアントを使用して次のステートメントを実行し、BE ノードの FQDN アクセスを有効にするために IP アドレスを FQDN に置き換えます。

```SQL
ALTER SYSTEM MODIFY BACKEND HOST "<be_ip>" TO "<be_hostname>";
```

> **注意**
>
> FQDN アクセスが有効になった後、BE ノードを再起動する必要はありません。

## ロールバック

FQDN アクセスが有効な StarRocks クラスタを FQDN アクセスをサポートしない以前のバージョンにロールバックするには、まずクラスタ内のすべてのノードに対して IP アドレスアクセスを有効にする必要があります。一般的なガイダンスについては [既存のクラスタで FQDN アクセスを有効にする](#enable-fqdn-access-in-an-existing-cluster) を参照してください。ただし、SQL コマンドを次のものに変更する必要があります。

- FE ノードの IP アドレスアクセスを有効にする:

```SQL
ALTER SYSTEM MODIFY FRONTEND HOST "<fe_hostname>" TO "<fe_ip>";
```

- BE ノードの IP アドレスアクセスを有効にする:

```SQL
ALTER SYSTEM MODIFY BACKEND HOST "<be_hostname>" TO "<be_ip>";
```

クラスタが正常に再起動された後に変更が有効になります。

## FAQ

**Q: FE ノードの FQDN アクセスを有効にするときにエラーが発生しました: "required 1 replica. But none were active with this master". どうすればよいですか？**

A: FE ノードの FQDN アクセスを有効にする前に、クラスタに少なくとも 3 つの Follower FE ノードがあることを確認してください。

**Q: FQDN アクセスが有効なクラスタに IP アドレスを使用して新しいノードを追加できますか？**

A: はい。