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

package com.starrocks.http.rest;

import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableProperty;
import com.starrocks.common.DdlException;
import com.starrocks.common.NotImplementedException;
import com.starrocks.common.StarRocksHttpException;
import com.starrocks.http.ActionController;
import com.starrocks.http.BaseRequest;
import com.starrocks.http.BaseResponse;
import com.starrocks.http.IllegalArgException;
import com.starrocks.http.model.TableDetailResult;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TableDetailAction extends RestBaseAction {

    private static final Logger LOG = LogManager.getLogger(TableDetailAction.class);

    private static final String PARAM_WITH_MV = "with_mv";
    private static final String PARAM_WITH_PROPERTY = "with_property";

    public TableDetailAction(ActionController controller) {
        super(controller);
    }

    public static void registerAction(ActionController controller) throws IllegalArgException {
        controller.registerHandler(HttpMethod.GET, "/api/v1/databases/{" + DB_KEY + "}/tables/{" + TABLE_KEY + "}",
                new TableDetailAction(controller));
        controller.registerHandler(HttpMethod.GET,
                "/api/v1/catalogs/{" + CATALOG_KEY + "/databases/{" + DB_KEY + "}/tables/{" + TABLE_KEY + "}",
                new TableDetailAction(controller));
    }

    @Override
    protected void executeWithoutPassword(BaseRequest request, BaseResponse response) throws DdlException {
        // just allocate 2 slot for top holder map
        TableDetailResult result = null;
        String catalogName = request.getSingleParameter(CATALOG_KEY);
        catalogName = Optional.ofNullable(catalogName).orElse(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME);
        if (!catalogName.equals(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME)) {
            throw new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "only support default_catalog right now");
        }
        String dbName = request.getSingleParameter(DB_KEY);
        String tableName = request.getSingleParameter(TABLE_KEY);
        String withMvPara = request.getSingleParameter(PARAM_WITH_MV);
        boolean withMv = isParameterTrue(withMvPara);
        String withPropertyPara = request.getSingleParameter(PARAM_WITH_PROPERTY);
        boolean withProperty = isParameterTrue(withPropertyPara);
        Optional.ofNullable(dbName).orElseThrow(
                () -> new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "No database selected."));
        Optional.ofNullable(tableName).orElseThrow(
                () -> new StarRocksHttpException(HttpResponseStatus.BAD_REQUEST, "No table selected."));
        // check privilege for select, otherwise return 401 HTTP status
        checkTableAction(ConnectContext.get(), dbName, tableName, PrivilegeType.SELECT);
        Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
        if (db == null) {
            throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                    "Database [" + dbName + "] " + "does not exists");
        }
        db.readLock();
        try {
            Table table = db.getTable(tableName);
            if (table == null) {
                throw new StarRocksHttpException(HttpResponseStatus.NOT_FOUND,
                        "Table [" + tableName + "] " + "does not exists");
            }

            TableDetailResult.TableSchemaInfoDto tableSchemaInfoDto = new TableDetailResult.TableSchemaInfoDto();
            tableSchemaInfoDto.setEngineType(table.getType().toString());

            TableDetailResult.SchemaInfoDto schemaInfo = generateSchemaInfo(table, withMv);
            tableSchemaInfoDto.setSchemaInfo(schemaInfo);

            fillMoreOlapMetaInfo(table, tableSchemaInfoDto, withProperty);
            result = new TableDetailResult(tableSchemaInfoDto);

        } finally {
            db.readUnlock();
        }

        // send result with extra information
        response.setContentType("application/json");
        response.getContent().append(result.toJson());
        sendResult(request, response, result);
    }

    private TableDetailResult.SchemaInfoDto generateSchemaInfo(Table tbl, boolean withMv) {
        TableDetailResult.SchemaInfoDto schemaInfoDto = new TableDetailResult.SchemaInfoDto();
        Map<String, TableDetailResult.TableSchemaDto> schemaMap = Maps.newHashMap();
        if (tbl.getType() == Table.TableType.OLAP) {
            OlapTable olapTable = (OlapTable) tbl;
            long baseIndexId = olapTable.getBaseIndexId();
            TableDetailResult.TableSchemaDto baseTableSchemaDto = new TableDetailResult.TableSchemaDto();
            baseTableSchemaDto.setBaseIndex(true);
            baseTableSchemaDto.setKeyType(olapTable.getKeysTypeByIndexId(baseIndexId).name());
            List<TableDetailResult.SchemaDto> baseSchema = generateSchema(olapTable.getSchemaByIndexId(baseIndexId));
            baseTableSchemaDto.setSchemaList(baseSchema);
            schemaMap.put(olapTable.getIndexNameById(baseIndexId), baseTableSchemaDto);

            if (withMv) {
                for (long indexId : olapTable.getIndexIdListExceptBaseIndex()) {
                    TableDetailResult.TableSchemaDto tableSchemaDto = new TableDetailResult.TableSchemaDto();
                    tableSchemaDto.setBaseIndex(false);
                    tableSchemaDto.setKeyType(olapTable.getKeysTypeByIndexId(indexId).name());
                    List<TableDetailResult.SchemaDto> schema = generateSchema(olapTable.getSchemaByIndexId(indexId));
                    tableSchemaDto.setSchemaList(schema);
                    schemaMap.put(olapTable.getIndexNameById(indexId), tableSchemaDto);
                }
            }

        } else {
            TableDetailResult.TableSchemaDto tableSchemaDto = new TableDetailResult.TableSchemaDto();
            tableSchemaDto.setBaseIndex(false);
            List<TableDetailResult.SchemaDto> schema = generateSchema(tbl.getBaseSchema());
            tableSchemaDto.setSchemaList(schema);
            schemaMap.put(tbl.getName(), tableSchemaDto);
        }
        schemaInfoDto.setSchemaMap(schemaMap);
        return schemaInfoDto;
    }

    private List<TableDetailResult.SchemaDto> generateSchema(List<Column> columns) {
        return columns.stream().map(column -> {
            TableDetailResult.SchemaDto schemaDto = new TableDetailResult.SchemaDto();
            schemaDto.setField(column.getName());
            schemaDto.setType(column.getType().toString());
            schemaDto.setIsNull(String.valueOf(column.isAllowNull()));
            schemaDto.setDefaultVal(column.getDefaultValue());
            schemaDto.setKey(String.valueOf(column.isKey()));
            schemaDto.setAggrType(column.getAggregationType() == null
                    ? "None" : column.getAggregationType().toString());
            schemaDto.setComment(column.getComment());
            return schemaDto;
        }).collect(Collectors.toList());
    }

    private void fillMoreOlapMetaInfo(Table table, TableDetailResult.TableSchemaInfoDto tableSchemaInfoDto,
                                      boolean withProperty) {
        if (table.getType() == Table.TableType.OLAP) {
            OlapTable olapTable = (OlapTable) table;
            fillTableProperties(tableSchemaInfoDto, withProperty, olapTable.getTableProperty());
            fillPartitionInfo(tableSchemaInfoDto, olapTable.getPartitionInfo());
            fillDistributionInfo(tableSchemaInfoDto, olapTable.getDefaultDistributionInfo());
        }
    }

    private void fillTableProperties(TableDetailResult.TableSchemaInfoDto tableSchemaInfoDto, boolean withProperty,
                                     TableProperty tableProperty) {
        if (withProperty && tableProperty != null) {
            tableSchemaInfoDto.setProperties(tableProperty.getProperties());
        }
    }

    private void fillPartitionInfo(TableDetailResult.TableSchemaInfoDto tableSchemaInfoDto,
                                   PartitionInfo partitionInfo) {
        TableDetailResult.PartitionInfoDto partitionInfoDto = new TableDetailResult.PartitionInfoDto();
        partitionInfoDto.setPartitionType(partitionInfo.getType().toString());
        try {
            partitionInfoDto.setPartitionColumns(
                    partitionInfo.getPartitionColumns().stream().map(Column::getName).collect(Collectors.toList()));
        } catch (NotImplementedException e) {
            LOG.warn(e.getMessage());
        }
        tableSchemaInfoDto.setPartitionInfo(partitionInfoDto);
    }

    private void fillDistributionInfo(TableDetailResult.TableSchemaInfoDto tableSchemaInfoDto,
                                      DistributionInfo distributionInfo) {
        TableDetailResult.DistributionInfoDto distributionInfoDto = new TableDetailResult.DistributionInfoDto();
        distributionInfoDto.setDistributionInfoType(distributionInfo.getType().toString());

        if (distributionInfo instanceof HashDistributionInfo) {
            HashDistributionInfo hashDistributionInfo = (HashDistributionInfo) distributionInfo;
            distributionInfoDto.setDistributionColumns(hashDistributionInfo.getDistributionColumns().stream()
                    .map(Column::getName).collect(Collectors.toList()));
            distributionInfoDto.setBucketNum(distributionInfo.getBucketNum());
        } else if (distributionInfo instanceof RandomDistributionInfo) {
            distributionInfoDto.setBucketNum(distributionInfo.getBucketNum());
        }
        tableSchemaInfoDto.setDistributionInfo(distributionInfoDto);
    }

    private static boolean isParameterTrue(String parameter) {
        return "1".equals(parameter);
    }

}
