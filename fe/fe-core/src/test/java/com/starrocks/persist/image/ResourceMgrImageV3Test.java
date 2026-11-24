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

package com.starrocks.persist.image;

import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.HudiResource;
import com.starrocks.catalog.IcebergResource;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.OdbcCatalogResource;
import com.starrocks.catalog.Resource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.catalog.SparkResource;
import com.starrocks.common.Pair;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The RESOURCE_MGR section: the resources of a cluster saved as a v3 image and loaded back are the live resources,
 * as they are through the v2 image.
 */
public class ResourceMgrImageV3Test {
    private static final List<String> NAMES = List.of("spark0", "hive0", "hudi0", "iceberg0", "jdbc0", "odbc0");

    @TempDir
    Path tempDir;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.createDefaultCtx());
        // the spark resource refers to a broker, which must exist
        GlobalStateMgr.getCurrentState().getBrokerMgr().addBrokers("broker0",
                Collections.singletonList(Pair.create("127.0.0.1", 9000)));
        // every class, with the optional fields set
        starRocksAssert.withResource("CREATE EXTERNAL RESOURCE spark0 PROPERTIES (\"type\" = \"spark\","
                + " \"spark.master\" = \"yarn\", \"spark.submit.deployMode\" = \"cluster\","
                + " \"spark.executor.memory\" = \"1g\", \"spark.hadoop.yarn.resourcemanager.address\" = \"rm:8032\","
                + " \"spark.hadoop.fs.defaultFS\" = \"hdfs://nn:9000\","
                + " \"working_dir\" = \"hdfs://nn:9000/tmp/starrocks\", \"broker\" = \"broker0\","
                + " \"broker.username\" = \"u\", \"broker.password\" = \"p\")");
        starRocksAssert.withResource("CREATE EXTERNAL RESOURCE hive0 PROPERTIES (\"type\" = \"hive\","
                + " \"hive.metastore.uris\" = \"thrift://127.0.0.1:9083\")");
        starRocksAssert.withResource("CREATE EXTERNAL RESOURCE hudi0 PROPERTIES (\"type\" = \"hudi\","
                + " \"hive.metastore.uris\" = \"thrift://127.0.0.1:9083\")");
        starRocksAssert.withResource("CREATE EXTERNAL RESOURCE iceberg0 PROPERTIES (\"type\" = \"iceberg\","
                + " \"iceberg.catalog.type\" = \"hive\","
                + " \"iceberg.catalog.hive.metastore.uris\" = \"thrift://127.0.0.1:9083\")");
        starRocksAssert.withResource("CREATE EXTERNAL RESOURCE jdbc0 PROPERTIES (\"type\" = \"jdbc\","
                + " \"user\" = \"postgres\", \"password\" = \"changeme\","
                + " \"jdbc_uri\" = \"jdbc:postgresql://127.0.0.1:5432/jdbc_test\","
                + " \"driver_url\" = \"https://repo1.maven.org/maven2/org/postgresql/postgresql/42.3.3/postgresql-42.3.3.jar\","
                + " \"driver_class\" = \"org.postgresql.Driver\")");
        starRocksAssert.withResource("CREATE EXTERNAL RESOURCE odbc0 PROPERTIES (\"type\" = \"odbc_catalog\","
                + " \"host\" = \"127.0.0.1\", \"port\" = \"3306\", \"user\" = \"u\", \"password\" = \"p\","
                + " \"odbc_type\" = \"mysql\", \"driver\" = \"MySQL ODBC 8.0 Unicode Driver\")");
    }

    /** SHOW RESOURCES: one row per property of every resource. */
    private static List<String> sortedRows(List<List<String>> rows) {
        List<String> sorted = new ArrayList<>();
        for (List<String> row : rows) {
            sorted.add(String.join("\t", row));
        }
        Collections.sort(sorted);
        return sorted;
    }

    @Test
    public void testV3ImageMatchesV2() throws Exception {
        ResourceMgr live = GlobalStateMgr.getCurrentState().getResourceMgr();
        Assertions.assertEquals(NAMES.size(), live.getResourceNum());

        // reference: the v2 image round trip
        UtFrameUtils.PseudoImage v2Image = new UtFrameUtils.PseudoImage();
        live.saveResourcesV2(v2Image.getImageWriter());
        ResourceMgr v2Loaded = new ResourceMgr();
        v2Loaded.loadResourcesV2(v2Image.getMetaBlockReader());

        // the v3 image round trip
        Path file = tempDir.resolve("resource_mgr.image");
        try (OutputStream os = Files.newOutputStream(file);
                ImageWriterV3 writer = new ImageWriterV3(os, 1L)) {
            live.save(writer);
            writer.finish();
        }
        ResourceMgr v3Loaded = new ResourceMgr();
        try (ImageReaderV3 reader = new ImageReaderV3(file)) {
            List<IndexEntryPB> sections = reader.sections();
            Assertions.assertEquals(1, sections.size());
            Assertions.assertEquals(SectionId.RESOURCE_MGR.getValue(), sections.get(0).getType());
            Assertions.assertEquals(NAMES.size(), sections.get(0).getNumEntries());
            reader.load(v3Loaded.getImageV3Loaders());
        }

        // The live manager is the anchor for both loads: SHOW RESOURCES and every resource, class included.
        List<String> liveRows = sortedRows(live.getResourcesInfo());
        Assertions.assertTrue(liveRows.contains("spark0\tspark\tbroker\tbroker0"), liveRows.toString());
        Assertions.assertEquals(liveRows, sortedRows(v2Loaded.getResourcesInfo()));
        Assertions.assertEquals(liveRows, sortedRows(v3Loaded.getResourcesInfo()));
        Assertions.assertEquals(live.getResourceNum(), v2Loaded.getResourceNum());
        Assertions.assertEquals(live.getResourceNum(), v3Loaded.getResourceNum());
        Set<Class<?>> classes = new HashSet<>();
        for (String name : NAMES) {
            Resource liveResource = live.getResource(name);
            Assertions.assertNotNull(liveResource, name);
            ProtoInstanceAssertions.assertInstances(liveResource.toProto());
            classes.add(liveResource.getClass());
            for (ResourceMgr loaded : List.of(v2Loaded, v3Loaded)) {
                Resource resource = loaded.getResource(name);
                Assertions.assertNotNull(resource, name);
                Assertions.assertEquals(liveResource.getClass(), resource.getClass(), name);
                Assertions.assertEquals(liveResource.getType(), resource.getType(), name);
                GsonEquivalence.assertEquivalent(liveResource, resource);
            }
        }
        Assertions.assertEquals(Set.of(SparkResource.class, HiveResource.class, HudiResource.class, IcebergResource.class,
                JDBCResource.class, OdbcCatalogResource.class), classes);

        // the optional fields made the trip
        SparkResource spark = (SparkResource) v3Loaded.getResource("spark0");
        Assertions.assertTrue(spark.hasBroker());
        Assertions.assertEquals("broker0", spark.getBroker());
        Assertions.assertEquals("hdfs://nn:9000/tmp/starrocks", spark.getWorkingDir());
        Assertions.assertEquals("yarn", spark.getSparkConfigs().get("spark.master"));
        IcebergResource iceberg = (IcebergResource) v3Loaded.getResource("iceberg0");
        Assertions.assertEquals("thrift://127.0.0.1:9083", iceberg.getHiveMetastoreURIs());
        HiveResource hive = (HiveResource) v3Loaded.getResource("hive0");
        Assertions.assertEquals("thrift://127.0.0.1:9083", hive.getHiveMetastoreURIs());
        JDBCResource jdbc = (JDBCResource) v3Loaded.getResource("jdbc0");
        Assertions.assertEquals("org.postgresql.Driver", jdbc.getProperty("driver_class"));
        OdbcCatalogResource odbc = (OdbcCatalogResource) v3Loaded.getResource("odbc0");
        Assertions.assertEquals("3306", odbc.getProperties("port"));
    }
}
