<img referrerpolicy="no-referrer-when-downgrade" src="https://static.scarf.sh/a.png?x-pxid=c615c99c-0f73-4a8b-af76-1e7ab83b90b5" />

# Docker Compose for StarRocks

This directory contains the Docker Compose YAML files for the StarRocks deployment.

You can deploy a StarRocks cluster with one BE node using [**docker-compose.yml**](./docker-compose.yml), or  simulate a distributed StarRocks cluster with multiple BEs on a single instance using [docker-compose-3BE.yml](./docker-compose-3BE.yml).

Note that deploying with docker compose is only recommended in a testing environment, as high availability cannot be guaranteed with a single instance deployment.
## Deploy StarRocks using Docker Compose

Run the following command to deploy StarRocks using Docker Compose:

```shell
docker-compose up -d
```

The commented-out sections in the above example YAML file define the mount paths and volumes that FE and BE use to persist data.
Note that root privilege is required to deploy StarRocks with Docker with persistent volume. 

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
ADD BACKEND "<be_ip>:9050";
```

Replace `<be_ip>` with the actual IP address of the BE node.
