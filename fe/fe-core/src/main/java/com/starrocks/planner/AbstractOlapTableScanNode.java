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

package com.starrocks.planner;

import com.starrocks.catalog.MaterializedIndexMeta;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.SchemaInfo;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.thrift.TTableSchemaKey;

import java.util.Optional;
import javax.annotation.Nullable;

// An abstract node to scan data or meta from an OlapTable.
public abstract class AbstractOlapTableScanNode extends ScanNode{

    protected final OlapTable olapTable;
    protected final SelectedMaterializedIndex index;

    /**
     * Constructs a node to scan from an OlapTable.
     * <p>
     * This constructor may access the schema information of the {@link OlapTable}.
     * To ensure thread-safe access, the caller must guarantee that the table's metadata
     * is protected, either by holding an external lock or by operating on a query-specific
     * copy of the table (e.g., created via {@link OlapTable#copyOnlyForQuery(OlapTable)}).
     *
     * @param id The unique identifier for this plan node.
     * @param desc The tuple descriptor for the data produced by this scan node.
     * @param planNodeName The name of the plan node.
     * @param selectedIndexId The ID of the materialized index to be scanned. If this value is -1,
     *                        the base index of the OlapTable will be used.
     */
    public AbstractOlapTableScanNode(
            PlanNodeId id, TupleDescriptor desc, String planNodeName, OlapTable olapTable, long selectedIndexId) {
        super(id, desc, planNodeName);
        this.olapTable = olapTable;
        this.index = SelectedMaterializedIndex.build(olapTable, selectedIndexId);
    }

    public OlapTable getOlapTable() {
        return olapTable;
    }

    public TTableSchemaKey getSchemaKey() {
        TTableSchemaKey schemaKey = new TTableSchemaKey();
        schemaKey.setDb_id(MetaUtils.lookupDbIdByTable(olapTable));
        schemaKey.setTable_id(olapTable.getId());
        schemaKey.setSchema_id(index.schemaId);
        return schemaKey;
    }

    /**
     * Returns the cached schema information for the selected materialized index.
     */
    public Optional<SchemaInfo> getSchema() {
        return index.cachedSchema;
    }


    /** Selected materialized index to scan. */
    protected static class SelectedMaterializedIndex {

        final long indexId;
        final long schemaId;

        /**
         * Cached schema information used for query plan.
         * <p>
         * This field stores the schema information that was used when generating the plan, enabling
         * backend nodes to retrieve the exact schema from the frontend during query execution. This is
         * particularly important for Fast Schema Evolution scenarios where schema changes are not
         * immediately propagated schema metadata to backend nodes. Currently, this mechanism is only
         * used in shared-data mode.
         * </p>
         */
        private final Optional<SchemaInfo> cachedSchema;

        private SelectedMaterializedIndex(long indexId, long schemaId, @Nullable SchemaInfo cachedSchema) {
            this.indexId = indexId;
            this.schemaId = schemaId;
            this.cachedSchema = Optional.ofNullable(cachedSchema);
        }

        static SelectedMaterializedIndex build(OlapTable olapTable, long selectedIndexId) {
            long indexId = selectedIndexId == -1 ? olapTable.getBaseIndexMetaId() : selectedIndexId;
            MaterializedIndexMeta indexMeta = olapTable.getIndexMetaByIndexId(indexId);
            if (indexMeta == null) {
                throw new RuntimeException(String.format(
                        "can't find index, table name: %s, table id: %s, index id: %s",
                        olapTable.getName(), olapTable.getId(), indexId));
            }
            long schemaId = indexMeta.getSchemaId();
            SchemaInfo schema = null;
            if (olapTable.isCloudNativeTableOrMaterializedView()) {
                schema = SchemaInfo.fromMaterializedIndex(olapTable, indexId, indexMeta);
            }
            return new SelectedMaterializedIndex(indexId, schemaId, schema);
        }
    }
}
