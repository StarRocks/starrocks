# Docker Compose for StarRocks

This directory contains the Docker Compose YAML files for the StarRocks deployment.

You can deploy a StarRocks cluster with one BE node using [**docker-compose.yaml**](./docker-compose.yaml), or a StarRocks cluster with three BE nodes using [docker-compose-multiBE.yaml](./docker-compose-multiBE.yaml).

## Deploy StarRocks using Docker Compose

Run the following command to deploy StarRocks using Docker Compose:

```shell
docker-compose up -d
```

## Docker Compose YAML files

The following example shows the Docker Compose YAML file that deploys a StarRocks cluster with one BE node:

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

The commented-out sections in the above example YAML file define the mount paths and volumes that FE and BE use to persist data. Note that root privilege is required to deploy StarRocks with Docker with persistent volume. 

## Check cluster status

After StarRocks is deployed, check the cluster status:

1. Connect to the cluster with the IP address of the FE instance. You can get the IP address of an instance using `docker inspect`.

  ```shell
  mysql -h <fe_ip> -P9030 -uroot
  ```

2. Check the status of the BE node.

  ```shell
  show backends;
  ```

  If the field Alive is true, this BE node is properly started and added to the cluster.

## Troubleshooting

When you connect to the cluster, StarRocks may return the following error:

```shell
 ERROR 2003 (HY000): Can't connect to MySQL server on 'starrocks-fe:9030' (111)
```

The reason may be that the BE node was started before the FE node is ready. To solve this problem, re-run the docker compose up command, or manually add the BE node to the cluster using the following command:

```sql
ADD BACKEDN "<be_ip>:9050";
```

Replace `<be_ip>` with the actual IP address of the BE node.
