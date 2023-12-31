# StarRocks Connector for Trino
This is a connector for StarRocks that is compatible with [Trino](https://trino.io/).

## Getting started
Ensure your system has a compatible Java runtime environment:
- Preferably, a 64-bit version of Java 17, with a minimum required version of 17.0.3.

Building from source code can be performed as below:
```bash
mvn clean package 
```

## How to use
- extract the plugin ZIP file `target/trino-starrocks-418.zip` and move the extracted directory into the Trino plugin directory.
- configure the catalog properties. An example can be found here: [[example](./examples/starrocks.properties)]. The configuration should be added to the Trino etc directory.

### Basic example
1. Create a docker network `trino_connector_network`.
```
docker network create trino_connector_network
```

2. Start up StarRocks locally.
```
docker run -p 9030:9030 -p 8030:8030 -p 8040:8040 -p 8080:8080 --privileged=true -itd --name starrocks --network trino_connector_network starrocks/allin1-ubuntu:3.2.1
```

3. Start Trino locally using the compiled jars.
```
docker run -p 8085:8080 -d --rm -v $PWD/target/trino-starrocks-418:/usr/lib/trino/plugin/trino-starrocks -v $PWD/examples/starrocks.properties:/etc/trino/catalog/starrocks.properties --name trino --network trino_connector_network trinodb/trino:418
```

4. At times, you might encounter an [issue](https://github.com/StarRocks/starrocks/issues/195) where the StarRocks table isn't visible in Trino. You can fix this by running the following command in StarRocks:
```sql
set global enable_groupby_use_output_alias = true;
```

## Known issues and limitations
This connector does not support query or task retries. Therefore, the [retry-policy](https://trino.io/docs/current/admin/properties-query-management.html#retry-policy) should be set to NONE.

Reading millisecond-level datetime types from StarRocks is currently not supported.

Additionally, this connector was developed based on Trino version 418 and not the latest version.

## Resources
For further details, refer to the [Trino documentation](https://trino.io/docs/current/).