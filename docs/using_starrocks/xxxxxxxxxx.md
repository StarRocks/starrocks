```sequence
user -> CatalogMgr: create catalog or resource
CatalogMgr -> ConnectorMgr: create connector
ConnectorMgr -> ConnectorMgr: create Hive Connector
ConnectorMgr -> HiveConnector: init hive connector
HiveConnector -> HiveConnector: constructor HiveMetadataFactory
HiveConnector -> user:
```




```sequence
QueryAnalyze -> MetadataMgr: getTable(catalogName, dbName);
MetadataMgr -> MetadataMgr: getConnectorMetadata();
MetadataMgr -> MetadataMgr: registerMetadata(queryId, catalogName);
MetadataMgr -> ConnectorMgr: getConnector(catalogName);
ConnectorMgr -> MetadataMgr: HiveConnector;
MetadataMgr -> HiveConnector: getConnectorMetadata;
HiveConnector -> HiveMetadataFatcoty: create();
HiveMetadataFatcoty -> HiveMetadataFatcoty: init HMSOps: HMSOps(new CachingHiveMetastore \n (metastoreFacotry.createMetastore()))
HiveMetadataFatcoty -> HiveMetadataFatcoty: init fileSystemOptions \n and metaStatsProvider;
HiveMetadataFatcoty -> MetadataMgr: getHiveMetadata;
MetadataMgr -> HiveMetadata: getTables;
HiveMetadata -> HMSOptions: getTables;
HMSOptions -> CachingHiveMetastore: getTables;
CachingHiveMetastore -> BaseCachingHiveMetastore: loadTables;
BaseCachingHiveMetastore -> LaunchingHiveMetastore: getTables;
LaunchingHiveMetastore -> HiveMetaClient : getTables;
HiveMetaClient -> HiveMetaThriftClient: getTables;
HiveMetaThriftClient -> HiveMetadata: List<string> tables;
HiveMetadata -> QueryAnalyze: List<String> tables;


```