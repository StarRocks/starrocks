:::note

ステージング環境の StarRocks クラスターに BE が 1 つしか含まれていない場合、`PROPERTIES` 句でレプリカ数を `1` に設定できます。例えば、`PROPERTIES( "replication_num" = "1" )` のようにします。デフォルトのレプリカ数は 3 であり、これは本番環境の StarRocks クラスターに推奨される数でもあります。デフォルトの数を使用したい場合は、`replication_num` パラメータを設定する必要はありません。

:::