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

package com.starrocks.connector.delta;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.thrift.TCloudConfiguration;
import com.starrocks.thrift.TCloudType;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static com.starrocks.connector.delta.UnityCatalogTestSupport.SAS_KEY;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.STORAGE;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.TOKEN;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.credentials;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.sas;
import static com.starrocks.connector.delta.UnityCatalogTestSupport.table;

public class UnityCatalogClientTest {
    private UnityCatalogTestSupport.FakeCatalog catalog;

    @BeforeEach
    public void setUp() throws IOException {
        catalog = new UnityCatalogTestSupport.FakeCatalog();
    }

    @AfterEach
    public void tearDown() {
        catalog.close();
    }

    @Test
    public void testForwardsFullCallerTokenAndRequestsTableScopedReadCredentials() {
        String location = STORAGE + "/alpha";
        String sas = sas("alpha-signature");
        catalog.enqueueTable("alpha", location, "?" + sas);

        MetastoreTable table = new UnityCatalogClient(catalog.properties(), TOKEN).getMetastoreTable("schema", "alpha");

        Assertions.assertEquals(location, table.getTableLocation());
        Assertions.assertEquals(1700000000L, table.getCreateTime());
        Assertions.assertEquals(List.of("Bearer " + TOKEN, "Bearer " + TOKEN), catalog.authorizations());
        Assertions.assertEquals("GET", catalog.requests.get(0).method());
        Assertions.assertEquals("/api/2.1/unity-catalog/tables/main.schema.alpha", catalog.requests.get(0).uri().getPath());
        Assertions.assertEquals("POST", catalog.requests.get(1).method());
        Assertions.assertEquals("/api/2.1/unity-catalog/temporary-table-credentials", catalog.requests.get(1).uri().getPath());
        JsonObject request = JsonParser.parseString(catalog.requests.get(1).body()).getAsJsonObject();
        Assertions.assertEquals("id-alpha", request.get("table_id").getAsString());
        Assertions.assertEquals("READ", request.get("operation").getAsString());
        Assertions.assertEquals(2, request.size());

        Configuration conf = new Configuration(false);
        table.getCloudConfiguration().applyToConfiguration(conf);
        Assertions.assertEquals(sas, conf.get(SAS_KEY));
        Assertions.assertEquals("SAS", conf.get("fs.azure.account.auth.type.account.dfs.core.windows.net"));
        TCloudConfiguration thrift = new TCloudConfiguration();
        table.getCloudConfiguration().toThrift(thrift);
        Assertions.assertEquals(TCloudType.AZURE, thrift.getCloud_type());
        Assertions.assertEquals(sas, thrift.getCloud_properties().get(SAS_KEY));
        Assertions.assertFalse(thrift.getCloud_properties().values().stream().anyMatch(value -> value.contains(TOKEN)));
    }

    @Test
    public void testListsPagesAndOnlyDeltaTablesWithCallerToken() {
        catalog.enqueue(200, "{\"schemas\":[{\"name\":\"schema\",\"catalog_name\":\"main\"}],"
                + "\"next_page_token\":\"page+2\"}");
        catalog.enqueue(200, "{\"schemas\":[{\"name\":\"other\",\"catalog_name\":\"main\"}]}");
        catalog.enqueue(200, "{\"tables\":[" + table("alpha", STORAGE + "/alpha")
                + ",{\"name\":\"not_delta\",\"catalog_name\":\"main\",\"schema_name\":\"schema\","
                + "\"data_source_format\":\"PARQUET\"}]}");
        UnityCatalogClient client = new UnityCatalogClient(catalog.properties(), TOKEN);

        Assertions.assertEquals(List.of("schema", "other"), client.getAllDatabaseNames());
        Assertions.assertTrue(catalog.requests.get(1).uri().getRawQuery().contains("page_token=page%2B2"));
        Assertions.assertEquals(List.of("alpha"), client.getAllTableNames("schema"));
        Assertions.assertEquals(List.of("Bearer " + TOKEN, "Bearer " + TOKEN, "Bearer " + TOKEN), catalog.authorizations());
    }

    @Test
    public void testMissingTableDoesNotRequestCredentials() {
        catalog.enqueue(404, "{}");
        Assertions.assertNull(new UnityCatalogClient(catalog.properties(), TOKEN).getMetastoreTable("schema", "alpha"));
        Assertions.assertEquals(1, catalog.requests.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {401, 403, 500, 302})
    public void testHttpErrorsDoNotFollowRedirectsOrExposeResponseSecrets(int status) {
        catalog.enqueue(status, "synthetic-response-secret " + TOKEN);
        StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new UnityCatalogClient(catalog.properties(), TOKEN).getMetastoreTable("schema", "alpha"));
        Assertions.assertTrue(error.getMessage().contains(Integer.toString(status)));
        Assertions.assertFalse(error.toString().contains("synthetic-response-secret"));
        Assertions.assertFalse(error.toString().contains(TOKEN));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(1, catalog.requests.size());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {" ", "invalid\ntoken"})
    public void testMissingOrInvalidTokenFailsBeforeHttp(String token) {
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new UnityCatalogClient(catalog.properties(), token));
        Assertions.assertTrue(catalog.requests.isEmpty());
    }

    @ParameterizedTest
    @MethodSource("invalidMetadata")
    public void testRejectsInvalidMetadataBeforeRequestingCredentials(String response) {
        catalog.enqueue(200, response);
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new UnityCatalogClient(catalog.properties(), TOKEN).getMetastoreTable("schema", "alpha"));
        Assertions.assertEquals(1, catalog.requests.size());
    }

    static Stream<String> invalidMetadata() {
        JsonObject missingId = table("alpha", STORAGE + "/alpha");
        missingId.remove("table_id");
        JsonObject wrongFormat = table("alpha", STORAGE + "/alpha");
        wrongFormat.addProperty("data_source_format", "PARQUET");
        JsonObject wrongCatalog = table("alpha", STORAGE + "/alpha");
        wrongCatalog.addProperty("catalog_name", "foreign");
        return Stream.of("not-json synthetic-secret", "[]", missingId.toString(), wrongFormat.toString(),
                wrongCatalog.toString(), table("alpha", "s3://bucket/alpha").toString());
    }

    @ParameterizedTest
    @MethodSource("invalidCredentials")
    public void testRejectsMissingExpiredOrWrongScopeCredentials(String response) {
        catalog.enqueue(200, table("alpha", STORAGE + "/alpha").toString());
        catalog.enqueue(200, response);
        StarRocksConnectorException error = Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new UnityCatalogClient(catalog.properties(), TOKEN).getMetastoreTable("schema", "alpha"));
        Assertions.assertFalse(error.toString().contains("synthetic-secret"));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(2, catalog.requests.size());
    }

    static Stream<String> invalidCredentials() {
        String location = STORAGE + "/alpha";
        JsonObject expired = credentials("alpha", location, sas("synthetic-secret"));
        expired.addProperty("expiration_time", 1L);
        JsonObject missingSas = credentials("alpha", location, sas("synthetic-secret"));
        missingSas.remove("azure_user_delegation_sas");
        JsonObject writeSas = credentials("alpha", location, sas("synthetic-secret").replace("sp=rl", "sp=rwl"));
        JsonObject expiresAfterSas = credentials("alpha", location, sas("synthetic-secret"));
        expiresAfterSas.addProperty("expiration_time", expiresAfterSas.get("expiration_time").getAsLong() + 1);
        JsonObject expiresAfterSigningKey = credentials("alpha", location,
                sas("synthetic-secret") + "&ske=2098-01-01T00%3A00%3A00Z");
        JsonObject futureStart = credentials("alpha", location,
                sas("synthetic-secret") + "&st=2098-01-01T00%3A00%3A00Z");
        return Stream.of("{malformed synthetic-secret", expired.toString(), missingSas.toString(), writeSas.toString(),
                credentials("alpha", STORAGE + "/beta", sas("synthetic-secret")).toString(),
                credentials("beta", location, sas("synthetic-secret")).toString(),
                credentials("alpha", location, sas("synthetic-secret").replace("sr=d", "sr=c")).toString(),
                credentials("alpha", location, sas("synthetic-secret").replace("sdd=1", "sdd=0")).toString(),
                credentials("alpha", location, sas("synthetic-secret").replace("&sdd=1", "")).toString(),
                expiresAfterSas.toString(), expiresAfterSigningKey.toString(), futureStart.toString());
    }

    @Test
    public void testCredentialAuthorizationFailureDoesNotFallBackToCatalogCredentials() {
        catalog.enqueue(200, table("alpha", STORAGE + "/alpha").toString());
        catalog.enqueue(403, "synthetic-secret");
        Assertions.assertThrows(StarRocksConnectorException.class,
                () -> new UnityCatalogClient(catalog.properties(), TOKEN).getMetastoreTable("schema", "alpha"));
        Assertions.assertEquals(2, catalog.requests.size());
    }
}
