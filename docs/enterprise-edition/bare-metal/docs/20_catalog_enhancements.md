# Catalog enhancements

Compared to the open-source StarRocks, CelerData offers additional enterprise-grade capabilities that further power access to external data sources, including:

- Unified access from one CelerData cluster to multiple Hadoop clusters. CelerData allows for centralized creation of configuration files for multiple Hadoop clusters, each of which serves a specific external data source, within one CelerData cluster. These configuration files are used to create and configure external catalogs for accessing specific Hadoop clusters. CelerData currently supports the following types of external catalogs: Hive catalogs, Hudi catalogs, Iceberg catalogs, and Paimon catalogs.
- Automatic renewal of Kerberos tickets.
- Customized username for each external catalog to access a specific Hadoop cluster.

## Unified access to multiple Hadoop clusters

If you manage multiple Hadoop clusters, each serving a specific external data source, and need to access all these Hadoop clusters within a single CelerData cluster, you can create a folder for each Hadoop cluster in the **conf** directories of every FE and BE (or CN) and save the **core-site.xml** file and **hdfs-site.xml** file from the HDFS cluster of the Hadoop cluster to the new folder. Additionally, for Hive catalogs and Hudi catalogs, you also need to save the **hive-site.xml** file from the Hive metastore of the Hadoop cluster to the new folder.

The following example shows two folders, `profileA` and `profileB`, which hold the configuration files for two Hadoop clusters:

```bash
// fe/conf directory structure

├── core-site.xml
├── fe.conf
├── hadoop_env.sh
├── profileA
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── hive-site.xml
├── profileB
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── hive-site.xml
└── udf_security.policy

// be/conf directory structure

├── be.conf
├── be_test.conf
├── cn.conf
├── core-site.xml
├── hadoop_env.sh
├── log4j2.properties
├── profileA
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── hive-site.xml
├── profileB
│   ├── core-site.xml
│   ├── hdfs-site.xml
│   └── hive-site.xml
└── udf_security.policy
```

When creating an external catalog to access a specific external data source, you must use `hadoop.config.resources` to declare the paths to the configuration files for the Hadoop cluster that stores the data from the external data source. The following statements demonstrate the creation of two Hive catalogs (`hive_catalog1` and `hive_catalog2`), where `hive_catalog1` uses the configuration files from `profileA` and `hive_catalog2` uses the configuration files from `profileB`:

```sql
CREATE EXTERNAL CATALOG `hive_catalog1`
comment "hive on hadoop01"
PROPERTIES
(
    "hadoop.config.resources" = "profileA/core-site.xml,profileA/hdfs-site.xml,profileA/hive-site.xml",
    "hive.metastore.uris" = "thrift://xxx.xx.xx.xx:9083",
    "type"  =  "hive"
);

CREATE EXTERNAL CATALOG `hive_catalog2`
comment "hive on hadoop02"
PROPERTIES
(
    "hadoop.config.resources" = "profileB/core-site.xml,profileB/hdfs-site.xml,profileB/hive-site.xml",
    "hive.metastore.uris" = "thrift://xxx.xx.xx.xx:9083",
    "type" = "hive"
);
```

## Automatic renewal of Kerberos tickets

If Kerberos authentication is enabled for the HDFS cluster or Hive metastore of a Hadoop cluster, make sure that the configuration files for the Hadoop cluster include the necessary items for CelerData to automatically renew the Kerberos tickets.

- For access to Kerberized Hive metastore, make sure that the **hive-site.xml** file includes the following items:
  - `hive.metastore.sasl.enabled`: whether to enable SASL. Set the value to `true`.
  - `hive.metastore.kerberos.keytab.file`: the path to the Kerberos keytab file ("keytab" for short) that contains the Kerberos principal of the Hive metastore server.
  - `hive.metastore.client.kerberos.principal`: the Kerberos principal of the Hive metastore client.
  - `hive.metastore.kerberos.principal`: the Kerberos principal of the Hive metastore server.

- For access to Kerberized HDFS, make sure that the **core-site.xml** or **hdfs-site.xml** file includes the following items:
  - `hadoop.security.authorization`: the authentication method. Set the value to `kerberos`, which enables Kerberos authentication.
  - `dfs.namenode.kerberos.principal`: the Kerberos principal of the NameNode.
  - `dfs.datanode.kerberos.principal`: the Kerberos principal of the DataNode.
  - `hadoop.hdfs.kerberos.keytab.file`: the path to the Kerberos keytab file ("keytab" for short) that contains the Kerberos principal of the Hive metastore client.
  - `hadoop.hdfs.client.kerberos.principal`: the Kerberos principal of the HDFS client.
  -  `hadoop.hdfs.kerberos.keytab.file` and `hadoop.hdfs.client.kerberos.principal` are not standard Hadoop configuration items. If it is inconvenient for you to add these configuration items to the **core-site.xml** or **hdfs-site.xml** file, you can create a new configuration file (for example, `other.xml`) to save them. In this case, you will need to correctly configure `hadoop.config.resources` to declare the path to the new configuration file that holds these two configuration items when you create an external catalog.

A few configuration examples are as follows:

- **hive-site.xml**

  - ```XML
    <configuration>
    ...
        <property>
            <name>hive.metastore.sasl.enabled</name>
            <value>true</value>
        </property>
        <property>
            <name>hive.metastore.kerberos.keytab.file</name>
            <value>/home/.../data/spark.keytab</value>
        </property>
        <property>
            <name>hive.metastore.client.kerberos.principal</name>
            <value>spark/....@ABC.COM</value>
        </property>
        <property>
            <name>hive.metastore.kerberos.principal</name>
            <value>hive/...@ABC.COM</value>
        </property>
    ...
    <configuration>
    ```

- **core-site.xml** or **hdfs-site.xml**

  - ```XML
    <configuration>
    ...
        <property>
          <name>hadoop.security.authentication</name>
          <value>kerberos</value>
        </property>
        <property>
          <name>dfs.datanode.kerberos.principal</name>
          <value>hdfs/...@ABC.COM</value>
        </property>
        <property>
          <name>dfs.namenode.kerberos.principal</name>
          <value>hdfs/...@ABC.COM</value>
        </property>
        <property>
          <name>hadoop.hdfs.kerberos.keytab.file</name>
          <value>/home/.../data/spark.keytab</value>
        </property>
        <property>
          <name>hadoop.hdfs.client.kerberos.principal</name>
          <value>spark/...@ABC.COM</value>
        </property>
    ...    
    </configuration>
    ```

  -  If it is inconvenient for you to add `hadoop.hdfs.kerberos.keytab.file` and `hadoop.hdfs.client.kerberos.principal` to the **core-site.xml** or **hdfs-site.xml** file, you can create a new configuration file (for example, `other.xml`) to save these configuration items. Example:

  - ```XML
    <configuration>
        <property>
          <name>hadoop.hdfs.kerberos.keytab.file</name>
          <value>/home/.../data/spark.keytab</value>
        </property>
        <property>
          <name>hadoop.hdfs.client.kerberos.principal</name>
          <value>spark/...@ABC.COM</value>
        </property>
    </configuration>
    ```

  -  After you do so, you must correctly configure `hadoop.config.resources` to declare the path to the new configuration file that holds these two configuration items when you create an external catalog. Example:

  - ```sql
    CREATE EXTERNAL CATALOG `hive_catalog2`
    comment "hive on hadoop02"
    PROPERTIES
    (
        "hadoop.config.resources" = "profileA/core-site.xml,profileA/hdfs-site.xml,profileA/hive-site.xml,profileA/other.xml",
        "hive.metastore.uris" = "thrift://xxx.xx.xx.xx:9083",
        "type" = "hive"
    );
    ```

## Customized username for accessing Hadoop cluster

CelerData supports setting a username (`hadoop.username`) for each external catalog. You can create different external catalogs, each bound to a specific username, in order to access different Hadoop clusters using different usernames.

This feature is available only when Kerberos is disabled.

| Parameter       | Description                                                  |
| --------------- | ------------------------------------------------------------ |
| hadoop.username | The username that is used to access the Hive metastore and HDFS cluster of a Hadoop cluster. |

The following example creates an external catalog named `hive_catalog1`, which enables the user `spark` to access the Hive metastore and HDFS cluster of a Hadoop cluster:

```sql
CREATE EXTERNAL CATALOG `hive_catalog1`
comment "hive on hadoop01"
PROPERTIES
(
   "hive.metastore.uris" = "thrift://xxx.xx.xx.xxx:9083",
   "hadoop.username" = "spark",
   "type" = "hive"
);
```
