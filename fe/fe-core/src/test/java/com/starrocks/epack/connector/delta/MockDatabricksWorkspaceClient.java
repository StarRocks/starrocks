// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.connector.delta;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.CatalogsService;
import com.databricks.sdk.service.catalog.CreateCatalog;
import com.databricks.sdk.service.catalog.CreateSchema;
import com.databricks.sdk.service.catalog.DataSourceFormat;
import com.databricks.sdk.service.catalog.DeleteCatalogRequest;
import com.databricks.sdk.service.catalog.DeleteSchemaRequest;
import com.databricks.sdk.service.catalog.DeleteTableRequest;
import com.databricks.sdk.service.catalog.ExistsRequest;
import com.databricks.sdk.service.catalog.GetCatalogRequest;
import com.databricks.sdk.service.catalog.GetSchemaRequest;
import com.databricks.sdk.service.catalog.GetTableRequest;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import com.databricks.sdk.service.catalog.ListCatalogsResponse;
import com.databricks.sdk.service.catalog.ListSchemasRequest;
import com.databricks.sdk.service.catalog.ListSchemasResponse;
import com.databricks.sdk.service.catalog.ListSummariesRequest;
import com.databricks.sdk.service.catalog.ListTableSummariesResponse;
import com.databricks.sdk.service.catalog.ListTablesRequest;
import com.databricks.sdk.service.catalog.ListTablesResponse;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.SchemasService;
import com.databricks.sdk.service.catalog.TableExistsResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TableType;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.databricks.sdk.service.catalog.TablesService;
import com.databricks.sdk.service.catalog.UpdateCatalog;
import com.databricks.sdk.service.catalog.UpdateSchema;
import com.databricks.sdk.service.catalog.UpdateTableRequest;

import java.util.Arrays;
import java.util.Iterator;

public class MockDatabricksWorkspaceClient extends WorkspaceClient {

    public MockDatabricksWorkspaceClient(DatabricksConfig config) {
        super(config);
    }

    @Override
    public SchemasAPI schemas() {
        return new MockSchemasAPI(new MockSchemasService());
    }

    @Override
    public TablesAPI tables() {
        return new MockTablesAPI(new MockTablesService());
    }


    public static class MockCatalogAPI extends CatalogsAPI {
        public MockCatalogAPI(CatalogsService service) {
            super(service);
        }

        @Override
        public CatalogInfo get(String name) {
            CatalogInfo catalogInfo = new CatalogInfo();
            catalogInfo.setName("databricks_catalog");
            return catalogInfo;
        }
    }

    public static class MockCatalogsService implements CatalogsService {

        @Override
        public CatalogInfo create(CreateCatalog createCatalog) {
            return null;
        }

        @Override
        public void delete(DeleteCatalogRequest deleteCatalogRequest) {

        }

        @Override
        public CatalogInfo get(GetCatalogRequest getCatalogRequest) {
            return null;
        }

        @Override
        public ListCatalogsResponse list(ListCatalogsRequest listCatalogsRequest) {
            return null;
        }

        @Override
        public CatalogInfo update(UpdateCatalog updateCatalog) {
            return null;
        }
    }

    public class MockSchemasService implements SchemasService {

        @Override
        public SchemaInfo create(CreateSchema createSchema) {
            return null;
        }

        @Override
        public void delete(DeleteSchemaRequest deleteSchemaRequest) {

        }

        @Override
        public SchemaInfo get(GetSchemaRequest getSchemaRequest) {
            return null;
        }

        @Override
        public ListSchemasResponse list(ListSchemasRequest listSchemasRequest) {
            return null;
        }

        @Override
        public SchemaInfo update(UpdateSchema updateSchema) {
            return null;
        }
    }

    public static class MockSchemasAPI extends SchemasAPI {
        public MockSchemasAPI(SchemasService service) {
            super(service);
        }

        @Override
        public Iterable<SchemaInfo> list(String catalogName) {
            SchemaInfo schemaInfo1 = new SchemaInfo();
            schemaInfo1.setCatalogName("databricks_catalog");
            schemaInfo1.setName("db1");

            SchemaInfo schemaInfo2 = new SchemaInfo();
            schemaInfo2.setCatalogName("databricks_catalog");
            schemaInfo2.setName("db2");

            return new Iterable<SchemaInfo>() {
                @Override
                public Iterator<SchemaInfo> iterator() {
                    return Arrays.asList(schemaInfo1, schemaInfo2).iterator();
                }
            };
        }

        @Override
        public SchemaInfo get(String fullName) {
            String[] names = fullName.split("\\.");
            SchemaInfo schemaInfo = new SchemaInfo();
            schemaInfo.setCatalogName(names[0]);
            schemaInfo.setName(names[1]);
            schemaInfo.setStorageLocation("s3://bucket/path/to/" + names[1]);
            return schemaInfo;
        }
    }

    public class MockTablesService implements TablesService {

        @Override
        public void delete(DeleteTableRequest deleteTableRequest) {

        }

        @Override
        public TableExistsResponse exists(ExistsRequest existsRequest) {
            return null;
        }

        @Override
        public TableInfo get(GetTableRequest getTableRequest) {
            return null;
        }

        @Override
        public ListTablesResponse list(ListTablesRequest listTablesRequest) {
            return null;
        }

        @Override
        public ListTableSummariesResponse listSummaries(ListSummariesRequest listSummariesRequest) {
            return null;
        }

        @Override
        public void update(UpdateTableRequest updateTableRequest) {
        }
    }

    public static class MockTablesAPI extends TablesAPI {
        public MockTablesAPI(MockTablesService service) {
            super(service);
        }

        @Override
        public Iterable<TableInfo> list(String catalogName, String schemaName) {
            TableInfo tableInfo1 = new TableInfo();
            tableInfo1.setCatalogName("databricks_catalog");
            tableInfo1.setSchemaName("db1");
            tableInfo1.setName("table1");
            tableInfo1.setTableType(TableType.MANAGED);
            tableInfo1.setDataSourceFormat(DataSourceFormat.DELTA);

            TableInfo tableInfo2 = new TableInfo();
            tableInfo2.setCatalogName("databricks_catalog");
            tableInfo2.setSchemaName("db1");
            tableInfo2.setName("table2");
            tableInfo2.setTableType(TableType.MANAGED);
            tableInfo2.setDataSourceFormat(DataSourceFormat.DELTA);

            return new Iterable<TableInfo>() {
                @Override
                public Iterator<TableInfo> iterator() {
                    return Arrays.asList(tableInfo1, tableInfo2).iterator();
                }
            };
        }

        @Override
        public TableInfo get(String fullName) {
            String[] names = fullName.split("\\.");
            TableInfo tableInfo = new TableInfo();
            tableInfo.setCatalogName(names[0]);
            tableInfo.setSchemaName(names[1]);
            tableInfo.setName(names[2]);
            tableInfo.setCreatedAt(1000L);
            tableInfo.setStorageLocation("s3://bucket/path/to/" + names[2]);
            tableInfo.setTableType(TableType.MANAGED);
            tableInfo.setDataSourceFormat(DataSourceFormat.DELTA);
            return tableInfo;
        }
    }
}