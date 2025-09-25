# StarClient Python Implementation

## Build StarClient Wheel Package

```shell
# python2
pip2 install --user grpcio-tools wheel
PYTHON=python2 ./build_package.sh
pip2 install --user dist/starclient-*py2*.whl
```

```shell
# python3
pip3 install --user grpcio-tools wheel
PYTHON=python3 ./build_package.sh
pip3 install --user dist/starclient-*py3*.whl
```

## Use StarClient CLI

### CLI help

```shell
usage: starclient [-h] [-s SERVER] {add,ls,dumpmeta} ...

starclient

optional arguments:
  -h, --help            show this help message and exit
  -s SERVER, --server SERVER
                        starManager server address. (default: 127.0.0.1:6090)

sub commands:
  additional help

  {add,ls,dumpmeta}
```

### Add Node into ResourcePool

```shell
usage: starclient add node [-h] --host HOST -p PORT

optional arguments:
  -h, --help            show this help message and exit
  --host HOST           host to be added (ip or fqdn)
  -p PORT, --port PORT  host port to be added
```

### List WorkerGroup

```shell
usage: starclient ls workergroup [-h] [-g WORKERGROUPID] -i SERVICEID [-w]

optional arguments:
  -h, --help            show this help message and exit
  -g WORKERGROUPID, --workergroupid WORKERGROUPID
                        Optional, worker group id to be listed, if absent,
                        list all worker groups
  -i SERVICEID, --serviceid SERVICEID
                        service id which the group belongs to
  -w, --include-workerinfo
                        Optional, if provided, workers info in the group will
                        be also listed.
```

### List Shard

```shell
usage: starclient ls shard [-h] -d SHARDID -g WORKERGROUPID -i SERVICEID

optional arguments:
  -h, --help            show this help message and exit
  -d SHARDID, --shardid SHARDID
                        shard id to be listed (default: None)
  -g WORKERGROUPID, --workergroupid WORKERGROUPID
                        worker group id to be listed (default: 0)
  -i SERVICEID, --serviceid SERVICEID
                        service id which the group belongs to (default: None)
```

### List Service
```shell
usage: starclient ls service [-h] [-i SERVICEID] [-n SERVICENAME]

list service by service id or by service name

optional arguments:
  -h, --help            show this help message and exit
  -i SERVICEID, --serviceid SERVICEID
                        service id to be listed (default: None)
  -n SERVICENAME, --servicename SERVICENAME
                        service name to be listed. (default: starrocks)
```

### Dump StarMgr meta

```shell
usage: starclient dumpmeta [-h]

optional arguments:
  -h, --help  show this help message and exit

```
