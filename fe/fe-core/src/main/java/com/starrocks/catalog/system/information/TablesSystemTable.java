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
package com.starrocks.catalog.system.information;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.service.InformationSchemaDataSource;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TUserIdentity;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.meta_data.FieldValueMetaData;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

public class TablesSystemTable extends SystemTable {
    public static final int MY_CS_NAME_SIZE = 32;
    private static final String NAME = "tables";

    private static final Logger LOG = LogManager.getLogger(TablesSystemTable.class);

    public TablesSystemTable(String catalogName) {
        super(catalogName, SystemId.TABLES_ID, NAME, Table.TableType.SCHEMA, builder()
                .column("TABLE_CATALOG", ScalarType.createVarchar(FN_REFLEN))
                .column("TABLE_SCHEMA", ScalarType.createVarchar(NAME_CHAR_LEN))
                .column("TABLE_NAME", ScalarType.createVarchar(NAME_CHAR_LEN))
                .column("TABLE_TYPE", ScalarType.createVarchar(NAME_CHAR_LEN))
                .column("ENGINE", ScalarType.createVarchar(NAME_CHAR_LEN))
                .column("VERSION", ScalarType.createType(PrimitiveType.BIGINT))
                .column("ROW_FORMAT", ScalarType.createVarchar(10))
                .column("TABLE_ROWS", ScalarType.createType(PrimitiveType.BIGINT))
                .column("AVG_ROW_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                .column("DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                .column("MAX_DATA_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                .column("INDEX_LENGTH", ScalarType.createType(PrimitiveType.BIGINT))
                .column("DATA_FREE", ScalarType.createType(PrimitiveType.BIGINT))
                .column("AUTO_INCREMENT", ScalarType.createType(PrimitiveType.BIGINT))
                .column("CREATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                .column("UPDATE_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                .column("CHECK_TIME", ScalarType.createType(PrimitiveType.DATETIME))
                .column("TABLE_COLLATION", ScalarType.createVarchar(MY_CS_NAME_SIZE))
                .column("CHECKSUM", ScalarType.createType(PrimitiveType.BIGINT))
                .column("CREATE_OPTIONS", ScalarType.createVarchar(255))
                .column("TABLE_COMMENT", ScalarType.createVarchar(2048))
                .build(), TSchemaTableType.SCH_TABLES);
    }

    public static SystemTable create(String catalogName) {
        return new TablesSystemTable(catalogName);
    }

    private static final Set<String> SUPPORTED_EQUAL_COLUMNS =
            Collections.unmodifiableSet(new TreeSet<>(String.CASE_INSENSITIVE_ORDER) {
                {
                    add("TABLE_SCHEMA");
                    add("TABLE_NAME");
                }
            });

    @Override
    public boolean supportFeEvaluation(ScalarOperator predicate) {
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        if (conjuncts.isEmpty()) {
            return true;
        }
        if (!isEmptyOrOnlyEqualConstantOps(conjuncts)) {
            return false;
        }
        return isSupportedEqualPredicateColumn(conjuncts, SUPPORTED_EQUAL_COLUMNS);
    }

    /**
     * To be compatible with BE's implementation, treat some special values as constant null.
     */
    private static boolean isConstantNullValue(Type valueType, Object object) {
        if (valueType.isStringType() && object == null) {
            return true;
        } else if (valueType.isBigint() && (Long) object == InformationSchemaDataSource.DEFAULT_EMPTY_NUM) {
            return true;
        }
        return false;
    }

    public static List<ScalarOperator> infoToScalar(SystemTable systemTable,
                                                    TTableInfo tableInfo) {
        List<ScalarOperator> result = Lists.newArrayList();
        for (Column column : systemTable.getBaseSchema()) {
            String name = column.getName().toLowerCase();
            TTableInfo._Fields field = TTableInfo._Fields.findByName(name);
            Preconditions.checkArgument(field != null, "Unknown field: " + name);
            FieldValueMetaData meta = TTableInfo.metaDataMap.get(field).valueMetaData;
            Object obj = tableInfo.getFieldValue(field);
            Type valueType = thriftToScalarType(meta.type);
            ConstantOperator scalar;
            if (isConstantNullValue(valueType, obj)) {
                scalar = ConstantOperator.createNull(column.getType());
            } else {
                scalar = ConstantOperator.createNullableObject(obj, valueType);
                try {
                    scalar = mayCast(scalar, column.getType());
                } catch (Exception e) {
                    LOG.debug("Failed to cast scalar operator for column: {}, value: {}, type: {}",
                            column.getName(), obj, valueType, e);
                    scalar = ConstantOperator.createNull(column.getType());
                }
            }
            result.add(scalar);
        }
        return result;
    }

    @Override
    public List<List<ScalarOperator>> evaluate(ScalarOperator predicate) {
        final List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        ConnectContext context = Preconditions.checkNotNull(ConnectContext.get(), "not a valid connection");
        TUserIdentity userIdentity = UserIdentityUtils.toThrift(context.getCurrentUserIdentity());
        TGetTablesInfoRequest params = new TGetTablesInfoRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(userIdentity);
        authInfo.setCatalog_name(this.getCatalogName());
        if (InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME.equalsIgnoreCase(this.getCatalogName())) {
            authInfo.setPattern(context.getDatabase());
        }
        for (ScalarOperator conjunct : conjuncts) {
            BinaryPredicateOperator binary = (BinaryPredicateOperator) conjunct;
            ColumnRefOperator columnRef = binary.getChild(0).cast();
            String name = columnRef.getName();
            ConstantOperator value = binary.getChild(1).cast();
            switch (name.toUpperCase()) {
                case "TABLE_NAME":
                    params.setTable_name(value.getVarchar());
                    break;
                case "TABLE_SCHEMA":
                    authInfo.setPattern(value.getVarchar());
                    break;
                default:
                    throw new NotImplementedException("unsupported column: " + name);
            }
        }
        params.setAuth_info(authInfo);

        try {
            TGetTablesInfoResponse result = query(params);
            return result.getTables_infos().stream()
                    .map(t -> infoToScalar(this, t))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            LOG.warn("Failed to query tables ", e);
            // Return empty result if query failed
            return Lists.newArrayList();
        }
    }

    public static TGetTablesInfoResponse query(TGetTablesInfoRequest request) throws TException {
        return InformationSchemaDataSource.generateTablesInfoResponse(request);
    }
}
