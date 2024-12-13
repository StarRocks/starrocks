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

<<<<<<< HEAD

package com.starrocks.catalog;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.thrift.TPaimonTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.AbstractFileStoreTable;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.List;
=======
package com.starrocks.catalog;

import com.starrocks.analysis.DescriptorTable;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.planner.PaimonScanNode;
import com.starrocks.thrift.TPaimonTable;
import com.starrocks.thrift.TTableDescriptor;
import com.starrocks.thrift.TTableType;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.types.DataField;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
import java.util.stream.Collectors;

import static com.starrocks.connector.ConnectorTableId.CONNECTOR_ID_GENERATOR;

<<<<<<< HEAD

public class PaimonTable extends Table {
    private static final Logger LOG = LogManager.getLogger(PaimonTable.class);
    private final String catalogName;
    private final String databaseName;
    private final String tableName;
    private final Options paimonOptions;
    private final AbstractFileStoreTable paimonNativeTable;
    private final List<String> partColumnNames;
    private final List<String> paimonFieldNames;

    public PaimonTable(String catalogName, String dbName, String tblName, List<Column> schema,
                       Options paimonOptions, org.apache.paimon.table.Table paimonNativeTable, long createTime) {
=======
public class PaimonTable extends Table {
    private String catalogName;
    private String databaseName;
    private String tableName;
    private org.apache.paimon.table.Table paimonNativeTable;
    private List<String> partColumnNames;
    private List<String> paimonFieldNames;
    private Map<String, String> properties;

    public PaimonTable() {
        super(TableType.PAIMON);
    }

    public PaimonTable(String catalogName, String dbName, String tblName, List<Column> schema,
                       org.apache.paimon.table.Table paimonNativeTable, long createTime) {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        super(CONNECTOR_ID_GENERATOR.getNextId().asInt(), tblName, TableType.PAIMON, schema);
        this.catalogName = catalogName;
        this.databaseName = dbName;
        this.tableName = tblName;
<<<<<<< HEAD
        this.paimonOptions = paimonOptions;
        this.paimonNativeTable = (AbstractFileStoreTable) paimonNativeTable;
=======
        this.paimonNativeTable = paimonNativeTable;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        this.partColumnNames = paimonNativeTable.partitionKeys();
        this.paimonFieldNames = paimonNativeTable.rowType().getFields().stream()
                .map(DataField::name)
                .collect(Collectors.toList());
        this.createTime = createTime;
    }

<<<<<<< HEAD
=======
    @Override
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    public String getCatalogName() {
        return catalogName;
    }

<<<<<<< HEAD
    public String getDbName() {
        return databaseName;
    }

    public String getTableName() {
        return tableName;
    }

    public AbstractFileStoreTable getNativeTable() {
=======
    @Override
    public String getCatalogDBName() {
        return databaseName;
    }

    @Override
    public String getCatalogTableName() {
        return tableName;
    }

    public org.apache.paimon.table.Table getNativeTable() {
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        return paimonNativeTable;
    }

    @Override
    public String getUUID() {
        return String.join(".", catalogName, databaseName, tableName, Long.toString(createTime));
    }

    @Override
    public String getTableLocation() {
<<<<<<< HEAD
        return paimonNativeTable.location().toString();
=======
        if (paimonNativeTable instanceof DataTable) {
            return ((DataTable) paimonNativeTable).location().toString();
        } else {
            return paimonNativeTable.name().toString();
        }
    }

    @Override
    public Map<String, String> getProperties() {
        if (properties == null) {
            this.properties = new HashMap<>();
        }
        return properties;
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public List<String> getPartitionColumnNames() {
        return partColumnNames;
    }

    @Override
    public List<Column> getPartitionColumns() {
        List<Column> partitionColumns = new ArrayList<>();
        if (!partColumnNames.isEmpty()) {
            partitionColumns = partColumnNames.stream().map(this::getColumn)
                    .collect(Collectors.toList());
        }
        return partitionColumns;
    }

    public List<String> getFieldNames() {
        return paimonFieldNames;
    }

    @Override
    public boolean isUnPartitioned() {
<<<<<<< HEAD
        return partColumnNames.size() == 0;
=======
        return partColumnNames.isEmpty();
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
    }

    @Override
    public boolean isSupported() {
        return true;
    }

    @Override
    public TTableDescriptor toThrift(List<DescriptorTable.ReferencedPartitionInfo> partitions) {
        TPaimonTable tPaimonTable = new TPaimonTable();
<<<<<<< HEAD
        StringBuilder sb = new StringBuilder();
        for (String key : this.paimonOptions.keySet()) {
            sb.append(key).append("=").append(this.paimonOptions.get(key)).append(",");
        }
        String option = sb.substring(0, sb.length() - 1);

        tPaimonTable.setPaimon_options(option);
=======
        String encodedTable = PaimonScanNode.encodeObjectToString(paimonNativeTable);
        tPaimonTable.setPaimon_native_table(encodedTable);
        tPaimonTable.setTime_zone(TimeUtils.getSessionTimeZone());
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
        TTableDescriptor tTableDescriptor = new TTableDescriptor(id, TTableType.PAIMON_TABLE,
                fullSchema.size(), 0, tableName, databaseName);
        tTableDescriptor.setPaimonTable(tPaimonTable);
        return tTableDescriptor;
    }
<<<<<<< HEAD
=======

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PaimonTable that = (PaimonTable) o;
        return catalogName.equals(that.catalogName) &&
                databaseName.equals(that.databaseName) &&
                tableName.equals(that.tableName) &&
                createTime == that.createTime;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, tableName, createTime);
    }
>>>>>>> b42eff7ae3 ([Doc] Add meaning of 0 for variables (#53714))
}
