### BE の動的パラメータを設定する

`curl` コマンドを使用して、BE ノードの動的パラメータを設定できます。

```Shell
curl -XPOST http://be_host:http_port/api/update_config?<configuration_item>=<value>
```