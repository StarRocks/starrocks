# docker-compose
This is a docker compose instance for starrocks. The repo used for you learn have to use and test starrocks. Deploys starrocks and start one fe instance, three be instances.
To run starrocks on your linux machine:
```shell
docker compose up
```
## Cluster
Let's see our docker-compose.yaml first.
```yaml
version: "3.9"
services:
  starrocks-fe:
    image: starrocks/fe-ubuntu:2.5.0-rc03 
    hostname: starrocks-fe
    container_name: starrocks-fe
    #user: root
    command: /opt/starrocks/fe/bin/start_fe.sh
    ports:
      - 1030:8030
      - 2020:9020
      - 3030:9030
    volumes:
      - ../../conf/fe.conf:/opt/starrocks/fe/conf/fe.conf
      #- ./data/fe/meta:/opt/starrocks/fe/meta
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9030"]
      interval: 5s
      timeout: 5s
      retries: 30

  starrocks-be1:
    image: starrocks/be-ubuntu:2.5.0-rc03
    #user: root
    command:
      - /bin/bash
      - -c
      - |
        sleep 15s; mysql --connect-timeout 2 -h starrocks-fe -P9030 -uroot -e "alter system add backend \"starrocks-be1:9050\";"
        /opt/starrocks/be/bin/start_be.sh 

    hostname: starrocks-be1
    container_name: starrocks-be1
    depends_on:
      - "starrocks-fe"
    volumes:
      - ../../conf/be.conf:/opt/starrocks/be/conf/be.conf
      #- ./data/starrocks-be1/storage:/opt/starrocks/be/storage
```
We have 1 fe server and 1 be servers.
the part of annotation is about defines mount host paths or named volumes that be accessible by fe and be. the user start fe and be in docker is starrocks, so when mount volumes we must use root to start fe and be.
so, when want to use volumes must cancel annotations.  
If you want to use 3 be reference the [docker-compose-multiBE.yaml](./docker-compose-multiBE.yaml)
## Check available
after started the starrocks ,we should check the cluster status.
1. connect the cluster.
Get the IP address about fe through docker inspect. Connect the fe and check all be status.
```shell
mysql -h fe_IP -P9030 -uroot
```
use show backends check all be status. when be Alive status is true represents the be is ok.
```shell
show backends;
```
## Problems
when up the cluster print as belows:
```shell
 ERROR 2003 (HY000): Can't connect to MySQL server on 'starrocks-fe:9030' (111)
```
that may be be start quickly than fe. you can re-compose up or add the be to fe manually finish it. The command as follows when you connect the fe service.
```
   ADD BACKEDN "be_ip:9050";
```
so as the command you should replace the be_ip as real docker ip.
