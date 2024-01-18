// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/catalog/EsTable.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.catalog;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.analysis.DescriptorTable.ReferencedPartitionInfo;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.connector.elasticsearch.EsMajorVersion;
import com.starrocks.connector.elasticsearch.EsMetaStateTracker;
import com.starrocks.connector.elasticsearch.EsRestClient;
import com.starrocks.connector.elasticsearch.EsTablePartitions;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TEsTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.Adler32;

public class EsTable extends Table implements GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(EsTable.class);

    public static final Set<String> DEFAULT_DOCVALUE_DISABLED_FIELDS = new HashSet<>(Arrays.asList("text"));

    public static final String KEY_HOSTS = "hosts";
    public static final String KEY_USER = "user";
    public static final String KEY_PASSWORD = "password";
    public static final String KEY_INDEX = "index";
    public static final String KEY_TYPE = "type";
    public static final String KEY_CATALOG_TYPE = "es.type";
    public static final String KEY_TRANSPORT = "transport";
    public static final String KEY_VERSION = "version";
    public static final String KEY_DOC_VALUES_MODE = "doc_values_mode";

    public static final String KEY_TRANSPORT_HTTP = "http";
    public static final String KEY_TRANSPORT_THRIFT = "thrift";
    public static final String KEY_DOC_VALUE_SCAN = "enable_docvalue_scan";
    public static final String KEY_KEYWORD_SNIFF = "enable_keyword_sniff";
    public static final String KEY_MAX_DOCVALUE_FIELDS = "max_docvalue_fields";

    public static final String KEY_WAN_ONLY = "es.nodes.wan.only";
    public static final String KEY_ES_NET_SSL = "es.net.ssl";
    public static final String KEY_TIME_ZONE = "time_zone";

    // tableContext is used for being convenient to persist some configuration parameters uniformly
    @SerializedName(value = "tc")
    private Map<String, String> tableContext = new HashMap<>();

    // only save the partition definition, save the partition key,
    // partition list is got from es cluster dynamically and is saved in esTableState
    @SerializedName(value = "p")
    private PartitionInfo partitionInfo;

    private String hosts;
    private String[] seeds;
    private String userName = "";
    private String passwd = "";
    // index name can be specific index, wildcard matched or alias.
    private String indexName;

    // which type used for `indexName`
    private String mappingType = null;
    private String transport = "http";

    private EsTablePartitions esTablePartitions;

    // Whether to enable docvalues scan optimization for fetching fields more fast, default to true
    private boolean enableDocValueScan = true;
    // Whether to enable sniffing keyword for filtering more reasonable, default to true
    private boolean enableKeywordSniff = true;
    // if the number of fields which value extracted from `doc_value` exceeding this max limitation
    // would downgrade to extract value from `stored_fields`
    private int maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;

    // Solr doc_values vs stored_fields performance-smackdown indicate:
    // It is possible to notice that retrieving an high number of fields leads
    // to a sensible worsening of performance if DocValues are used.
    // Instead,  the (almost) surprising thing is that, by returning less than 20 fields,
    // DocValues performs better than stored fields and the difference gets little as the number of fields returned increases.
    // Asking for 9 DocValues fields and 1 stored field takes an average query time is 6.86 (more than returning 10 stored fields)
    // Here we have a slightly conservative value of 20, but at the same time we also provide configurable parameters for expert-using
    // @see `MAX_DOCVALUE_FIELDS`
    private static final int DEFAULT_MAX_DOCVALUE_FIELDS = 20;

    private boolean wanOnly = false;
    private boolean sslEnabled = false;
    private String timeZone = null;

    // version would be used to be compatible with different ES Cluster
    public EsMajorVersion majorVersion = null;

    // record the latest and recently exception when sync ES table metadata (mapping, shard location)
    private Throwable lastMetaDataSyncException = null;
    // used for catalog to identify the remote table.
    private String catalogName = null;
    private String dbName = null;

    public EsTable() {
        super(TableType.ELASTICSEARCH);
    }

    public EsTable(long id, String name, List<Column> schema, Map<String, String> properties,
                   PartitionInfo partitionInfo) throws DdlException {
        super(id, name, TableType.ELASTICSEARCH, schema);
        this.partitionInfo = partitionInfo;
        validate(properties);
    }

    public EsTable(long id, String catalogName, String dbName, String name, List<Column> schema, Map<String, String> properties,
                   PartitionInfo partitionInfo) throws DdlException {
        super(id, name, TableType.ELASTICSEARCH, schema);
        this.partitionInfo = partitionInfo;
        this.catalogName = catalogName;
        this.dbName = dbName;
        validate(properties);
    }

    public Map<String, String> fieldsContext() {
        return esMetaStateTracker.searchContext().fetchFieldsContext();
    }

    public Map<String, String> docValueContext() {
        return esMetaStateTracker.searchContext().docValueFieldsContext();
    }

    public int maxDocValueFields() {
        return maxDocValueFields;
    }

    public boolean isDocValueScanEnable() {
        return enableDocValueScan;
    }

    public boolean isKeywordSniffEnable() {
        return enableKeywordSniff;
    }

    public boolean wanOnly() {
        return wanOnly;
    }

    public boolean sslEnabled() {
        return sslEnabled;
    }

    private void validate(Map<String, String> properties) throws DdlException {
        if (properties == null) {
            throw new DdlException("Please set properties of elasticsearch table, "
                    + "they are: hosts, user, password, index");
        }

        if (Strings.isNullOrEmpty(properties.get(KEY_HOSTS))
                || Strings.isNullOrEmpty(properties.get(KEY_HOSTS).trim())) {
            throw new DdlException("Hosts of ES table is null. "
                    + "Please add properties('hosts'='xxx.xxx.xxx.xxx,xxx.xxx.xxx.xxx') when create table");
        }
        hosts = properties.get(KEY_HOSTS).trim();
        seeds = hosts.split(",");

        if (!Strings.isNullOrEmpty(properties.get(KEY_USER))
                && !Strings.isNullOrEmpty(properties.get(KEY_USER).trim())) {
            userName = properties.get(KEY_USER).trim();
        }

        if (!Strings.isNullOrEmpty(properties.get(KEY_PASSWORD))
                && !Strings.isNullOrEmpty(properties.get(KEY_PASSWORD).trim())) {
            passwd = properties.get(KEY_PASSWORD).trim();
        }

        if (Strings.isNullOrEmpty(properties.get(KEY_INDEX))
                || Strings.isNullOrEmpty(properties.get(KEY_INDEX).trim())) {
            throw new DdlException("Index of ES table is null. "
                    + "Please add properties('index'='xxxx') when create table");
        }
        indexName = properties.get(KEY_INDEX).trim();

        // Explicit setting for cluster version to avoid detecting version failure
        if (properties.containsKey(KEY_VERSION)) {
            try {
                majorVersion = EsMajorVersion.parse(properties.get(KEY_VERSION).trim());
                if (majorVersion.before(EsMajorVersion.V_5_X)) {
                    throw new DdlException("Unsupported/Unknown ES Cluster version [" + properties.get(KEY_VERSION) + "] ");
                }
            } catch (Exception e) {
                throw new DdlException("fail to parse ES major version, version= "
                        + properties.get(KEY_VERSION).trim() + ", should be like '6.5.3' ");
            }
        }

        // enable doc value scan for Elasticsearch
        if (properties.containsKey(KEY_DOC_VALUE_SCAN)) {
            try {
                enableDocValueScan = Boolean.parseBoolean(properties.get(KEY_DOC_VALUE_SCAN).trim());
            } catch (Exception e) {
                throw new DdlException("fail to parse enable_docvalue_scan, enable_docvalue_scan= "
                        + properties.get(KEY_VERSION).trim() + " ,`enable_docvalue_scan`"
                        + " shoud be like 'true' or 'false', value should be double quotation marks");
            }
        }

        if (properties.containsKey(KEY_KEYWORD_SNIFF)) {
            try {
                enableKeywordSniff = Boolean.parseBoolean(properties.get(KEY_KEYWORD_SNIFF).trim());
            } catch (Exception e) {
                throw new DdlException("fail to parse enable_keyword_sniff, enable_keyword_sniff= "
                        + properties.get(KEY_VERSION).trim() + " ,`enable_keyword_sniff`"
                        + " shoud be like 'true' or 'false', value should be double quotation marks");
            }
        } else {
            enableKeywordSniff = true;
        }

        if (!Strings.isNullOrEmpty(properties.get(KEY_TYPE))
                && !Strings.isNullOrEmpty(properties.get(KEY_TYPE).trim())) {
            // just for compatible external es table definition
            // type in catalog means that such as es/hive/iceberg, but type in table properties also can be defined.
            if (!"es".equalsIgnoreCase(properties.get(KEY_TYPE).trim())) {
                mappingType = properties.get(KEY_TYPE).trim();
            } else {
                if (!Strings.isNullOrEmpty(properties.get(KEY_CATALOG_TYPE))
                        && !Strings.isNullOrEmpty(properties.get(KEY_CATALOG_TYPE).trim())) {
                    mappingType = properties.get(KEY_CATALOG_TYPE).trim();
                }
            }
        } else {
            mappingType = null;
        }
        if (!Strings.isNullOrEmpty(properties.get(KEY_TRANSPORT))
                && !Strings.isNullOrEmpty(properties.get(KEY_TRANSPORT).trim())) {
            transport = properties.get(KEY_TRANSPORT).trim();
            if (!(KEY_TRANSPORT_HTTP.equals(transport) || KEY_TRANSPORT_THRIFT.equals(transport))) {
                throw new DdlException("transport of ES table must be http(recommend) or thrift(reserved inner usage),"
                        + " but value is " + transport);
            }
        }

        if (properties.containsKey(KEY_MAX_DOCVALUE_FIELDS)) {
            try {
                maxDocValueFields = Integer.parseInt(properties.get(KEY_MAX_DOCVALUE_FIELDS).trim());
                if (maxDocValueFields < 0) {
                    maxDocValueFields = 0;
                }
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }

        if (properties.containsKey(KEY_WAN_ONLY)) {
            try {
                wanOnly = Boolean.parseBoolean(properties.get(KEY_WAN_ONLY).trim());
            } catch (Exception e) {
                wanOnly = false;
            }
        }
        if (properties.containsKey(KEY_ES_NET_SSL)) {
            try {
                sslEnabled = Boolean.parseBoolean(properties.get(KEY_ES_NET_SSL).trim());
            } catch (Exception e) {
                sslEnabled = false;
            }
        }
        if (properties.containsKey(KEY_TIME_ZONE)) {
            timeZone = properties.get(KEY_TIME_ZONE).trim();
        }

        Column idColumn = getColumn("_id");
        if (idColumn != null && !(idColumn.getPrimitiveType() == PrimitiveType.VARCHAR
                || idColumn.getPrimitiveType() == PrimitiveType.CHAR)) {
            throw new DdlException("Type of _id (ES Primary-Key) Column must be Char/Varchar");
        }
        tableContext.put("hosts", hosts);
        tableContext.put("userName", userName);
        tableContext.put("passwd", passwd);
        tableContext.put("indexName", indexName);
        if (mappingType != null) {
            tableContext.put("mappingType", mappingType);
        }
        tableContext.put("transport", transport);
        if (majorVersion != null) {
            tableContext.put("majorVersion", majorVersion.toString());
        }
        tableContext.put("enableDocValueScan", String.valueOf(enableDocValueScan));
        tableContext.put("enableKeywordSniff", String.valueOf(enableKeywordSniff));
        tableContext.put("maxDocValueFields", String.valueOf(maxDocValueFields));
        tableContext.put("es.nodes.wan.only", String.valueOf(wanOnly));
        tableContext.put(KEY_ES_NET_SSL, String.valueOf(sslEnabled));
    }

    @Override
    public TTableDescriptor toThrift(List<ReferencedPartitionInfo> partitions) {
        TEsTable tEsTable = new TEsTable();
        TTableDescriptor tTableDescriptor = new TTableDescriptor(getId(), TTableType.ES_TABLE,
                fullSchema.size(), 0, getName(), "");
        tTableDescriptor.setEsTable(tEsTable);
        return tTableDescriptor;
    }

    // TODO, identify the remote table that created after deleted
    @Override
    public String getUUID() {
        if (!Strings.isNullOrEmpty(catalogName)) {
            return String.join(".", catalogName, dbName, name);
        } else {
            return Long.toString(id);
        }
    }

    @Override
    public int getSignature(int signatureVersion) {
        Adler32 adler32 = new Adler32();
        adler32.update(signatureVersion);

        // name
        adler32.update(name.getBytes(StandardCharsets.UTF_8));
        // type
        adler32.update(type.name().getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<String, String> entry : tableContext.entrySet()) {
            adler32.update(entry.getValue().getBytes(StandardCharsets.UTF_8));
        }

        return Math.abs((int) adler32.getValue());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        out.writeInt(tableContext.size());
        for (Map.Entry<String, String> entry : tableContext.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue());
        }
        Text.writeString(out, partitionInfo.getType().name());
        partitionInfo.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        int size = in.readInt();
        for (int i = 0; i < size; ++i) {
            String key = Text.readString(in);
            String value = Text.readString(in);
            tableContext.put(key, value);
        }
        hosts = tableContext.get("hosts");
        seeds = hosts.split(",");
        userName = tableContext.get("userName");
        passwd = tableContext.get("passwd");
        indexName = tableContext.get("indexName");
        mappingType = tableContext.get("mappingType");
        transport = tableContext.get("transport");
        if (tableContext.containsKey("majorVersion")) {
            try {
                majorVersion = EsMajorVersion.parse(tableContext.get("majorVersion"));
            } catch (Exception e) {
                majorVersion = EsMajorVersion.V_5_X;
            }
        }

        enableDocValueScan = Boolean.parseBoolean(tableContext.get("enableDocValueScan"));
        if (tableContext.containsKey("enableKeywordSniff")) {
            enableKeywordSniff = Boolean.parseBoolean(tableContext.get("enableKeywordSniff"));
        } else {
            enableKeywordSniff = true;
        }
        if (tableContext.containsKey("maxDocValueFields")) {
            try {
                maxDocValueFields = Integer.parseInt(tableContext.get("maxDocValueFields"));
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }
        if (tableContext.containsKey(KEY_WAN_ONLY)) {
            wanOnly = Boolean.parseBoolean(tableContext.get(KEY_WAN_ONLY));
        } else {
            wanOnly = false;
        }
        if (tableContext.containsKey(KEY_ES_NET_SSL)) {
            sslEnabled = Boolean.parseBoolean(tableContext.get(KEY_ES_NET_SSL));
        } else {
            sslEnabled = false;
        }

        PartitionType partType = PartitionType.valueOf(Text.readString(in));
        if (partType == PartitionType.UNPARTITIONED) {
            partitionInfo = SinglePartitionInfo.read(in);
        } else if (partType == PartitionType.RANGE) {
            partitionInfo = RangePartitionInfo.read(in);
        } else {
            throw new IOException("invalid partition type: " + partType);
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        super.gsonPostProcess();

        hosts = tableContext.get("hosts");
        seeds = hosts.split(",");
        userName = tableContext.get("userName");
        passwd = tableContext.get("passwd");
        indexName = tableContext.get("indexName");
        mappingType = tableContext.get("mappingType");
        transport = tableContext.get("transport");
        if (tableContext.containsKey("majorVersion")) {
            try {
                majorVersion = EsMajorVersion.parse(tableContext.get("majorVersion"));
            } catch (Exception e) {
                majorVersion = EsMajorVersion.V_5_X;
            }
        }

        enableDocValueScan = Boolean.parseBoolean(tableContext.get("enableDocValueScan"));
        if (tableContext.containsKey("enableKeywordSniff")) {
            enableKeywordSniff = Boolean.parseBoolean(tableContext.get("enableKeywordSniff"));
        } else {
            enableKeywordSniff = true;
        }
        if (tableContext.containsKey("maxDocValueFields")) {
            try {
                maxDocValueFields = Integer.parseInt(tableContext.get("maxDocValueFields"));
            } catch (Exception e) {
                maxDocValueFields = DEFAULT_MAX_DOCVALUE_FIELDS;
            }
        }
        if (tableContext.containsKey(KEY_WAN_ONLY)) {
            wanOnly = Boolean.parseBoolean(tableContext.get(KEY_WAN_ONLY));
        } else {
            wanOnly = false;
        }
        if (tableContext.containsKey(KEY_ES_NET_SSL)) {
            sslEnabled = Boolean.parseBoolean(tableContext.get(KEY_ES_NET_SSL));
        } else {
            sslEnabled = false;
        }
    }

    public String getHosts() {
        return hosts;
    }

    public String[] getSeeds() {
        return seeds;
    }

    public String getUserName() {
        return userName;
    }

    public String getPasswd() {
        return passwd;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getMappingType() {
        return mappingType;
    }

    public String getTransport() {
        return transport;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public PartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    public EsTablePartitions getEsTablePartitions() {
        return esTablePartitions;
    }

    public void setEsTablePartitions(EsTablePartitions esTablePartitions) {
        this.esTablePartitions = esTablePartitions;
    }

    public EsMajorVersion esVersion() {
        return majorVersion;
    }

    public Throwable getLastMetaDataSyncException() {
        return lastMetaDataSyncException;
    }

    public void setLastMetaDataSyncException(Throwable lastMetaDataSyncException) {
        this.lastMetaDataSyncException = lastMetaDataSyncException;
    }

    private EsMetaStateTracker esMetaStateTracker;

    /**
     * sync es index meta from remote ES Cluster
     *
     * @param client esRestClient
     */
    public void syncTableMetaData(EsRestClient client) throws Exception {
        if (esMetaStateTracker == null) {
            esMetaStateTracker = new EsMetaStateTracker(client, this);
        }
        esMetaStateTracker.run();
        this.esTablePartitions = esMetaStateTracker.searchContext().tablePartitions();
    }

    @Override
    public void onDrop(Database db, boolean force, boolean replay) {
        GlobalStateMgr.getCurrentState().getEsRepository().deRegisterTable(this.id);
    }

    @Override
    public void onReload() {
        GlobalStateMgr.getCurrentState().getEsRepository().registerTable(this);
    }

    @Override
    public boolean isSupported() {
        return true;
    }
}
