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

import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.HiveResource;
import com.starrocks.catalog.ResourceMgr;
import com.starrocks.connector.CatalogConnector;
import com.starrocks.connector.ConnectorContext;
import com.starrocks.connector.ConnectorMgr;
import com.starrocks.persist.metablock.SRMetaBlockReaderV2;
import com.starrocks.persist.proto.IndexEntryPB;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.getResourceMappingCatalogName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CatalogMgrImageV3Test {
    @TempDir
    Path tempDir;
    private GlobalStateMgr global;
    private ConnectorMgr connectors;
    private ResourceMgr resources;

    @BeforeEach
    public void setUp() {
        global = LoadImageTestSupport.global();
        connectors = mock(ConnectorMgr.class);
        when(connectors.createConnector(any(), eq(true))).thenAnswer(invocation -> mock(CatalogConnector.class));
        when(connectors.createHiddenConnector(any(), eq(false))).thenAnswer(invocation -> mock(CatalogConnector.class));
        resources = mock(ResourceMgr.class);
        when(global.getResourceMgr()).thenReturn(resources);
        when(resources.getNeedMappingCatalogResources()).thenReturn(List.of());
    }

    static Catalog catalog(long id, String name) {
        return new Catalog(id, name, new HashMap<>(Map.of(
                "type", "hive", "hive.metastore.uris", "thrift://127.0.0.1:9083", "custom", "value")), "catalog comment");
    }

    static void add(CatalogMgr manager, Catalog catalog) {
        Map<String, Catalog> catalogs = getField(manager, "catalogs");
        catalogs.put(catalog.getName(), catalog);
    }

    private CatalogMgr v2(CatalogMgr source) throws Exception {
        UtFrameUtils.PseudoImage image = new UtFrameUtils.PseudoImage();
        source.save(image.getImageWriter());
        CatalogMgr result = new CatalogMgr(connectors);
        SRMetaBlockReaderV2 reader = new SRMetaBlockReaderV2(image.getJsonReader());
        result.load(reader);
        reader.close();
        return result;
    }

    private CatalogMgr v3(CatalogMgr source) throws Exception {
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        CatalogMgr result = new CatalogMgr(connectors);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, result.getImageV3Loaders());
        assertEquals(210, sections.get(0).getType());
        assertEquals(0, sections.get(0).getManagerLength());
        return result;
    }

    @Test
    public void testCatalogsAndConnectorCreationMatchV2() throws Exception {
        CatalogMgr source = new CatalogMgr(connectors);
        add(source, catalog(11, "catalog1"));
        add(source, catalog(12, "catalog2"));
        CatalogMgr expected = v2(source);
        CatalogMgr restored = v3(source);
        for (String name : List.of("catalog1", "catalog2")) {
            GsonEquivalence.assertEquivalent(expected.getCatalogByName(name), restored.getCatalogByName(name));
            assertEquals("hive", restored.getCatalogType(name));
        }

        ArgumentCaptor<ConnectorContext> contexts = ArgumentCaptor.forClass(ConnectorContext.class);
        verify(connectors, times(4)).createConnector(contexts.capture(), eq(true));
        assertTrue(contexts.getAllValues().stream().allMatch(c -> c.getType().equals("hive")));
    }

    @Test
    public void testResourceMappingCatalogIsExcluded() throws Exception {
        CatalogMgr source = new CatalogMgr(connectors);
        String mapping = getResourceMappingCatalogName("resource", "hive");
        add(source, catalog(11, "catalog1"));
        add(source, catalog(12, mapping));
        CatalogMgr restored = v3(source);
        assertNotNull(restored.getCatalogByName("catalog1"));
        assertNull(restored.getCatalogByName(mapping));

    }

    @Test
    public void testFailedCatalogDoesNotStopLaterEntries() throws Exception {
        when(connectors.createConnector(any(), eq(true))).thenAnswer(invocation -> {
            ConnectorContext context = invocation.getArgument(0);
            return "bad".equals(context.getCatalogName()) ? null : mock(CatalogConnector.class);
        });
        Path path = LoadImageTestSupport.save(tempDir, writer -> {
            try (SectionWriter section = writer.beginSection(SectionId.CATALOG_MGR)) {
                section.writeEntry(catalog(1, "before").toProto());
                section.writeEntry(catalog(2, "bad").toProto());
                section.writeEntry(catalog(3, "after").toProto());
            }
        });
        CatalogMgr restored = new CatalogMgr(connectors);
        LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertNotNull(restored.getCatalogByName("before"));
        assertNull(restored.getCatalogByName("bad"));
        assertNotNull(restored.getCatalogByName("after"));

    }

    @Test
    public void testMissingAndUnsupportedConnectorTypesAreSkipped() throws Exception {
        CatalogMgr source = new CatalogMgr(connectors);
        Catalog missing = new Catalog(1, "missing", new HashMap<>(), "");
        Catalog unsupported = new Catalog(2, "unsupported", Map.of("type", "future_connector"), "");
        add(source, missing);
        add(source, unsupported);
        add(source, catalog(3, "valid"));
        CatalogMgr expected = v2(source);
        CatalogMgr restored = v3(source);
        assertNull(restored.getCatalogByName("missing"));
        assertNull(restored.getCatalogByName("unsupported"));
        GsonEquivalence.assertEquivalent(expected.getCatalogByName("valid"), restored.getCatalogByName("valid"));

    }

    @Test
    public void testMappingCatalogRebuiltAfterResourceLoad() throws Exception {
        HiveResource resource = mock(HiveResource.class);
        when(resource.getName()).thenReturn("resource");
        when(resource.getType()).thenReturn(com.starrocks.catalog.Resource.ResourceType.HIVE);
        when(resource.getProperties()).thenReturn(Map.of("hive.metastore.uris", "thrift://127.0.0.1:9083"));
        when(resource.getHiveMetastoreURIs()).thenReturn("thrift://127.0.0.1:9083");
        when(resources.getNeedMappingCatalogResources()).thenReturn(List.of(resource));
        String name = getResourceMappingCatalogName("resource", "hive");
        CatalogMgr source = new CatalogMgr(connectors);
        add(source, catalog(1, "normal"));
        CatalogMgr restored = v3(source);
        assertNotNull(restored.getCatalogByName("normal"));
        Catalog mapping = restored.getCatalogByName(name);
        assertNotNull(mapping);
        assertEquals("hive", mapping.getType());
        assertEquals("thrift://127.0.0.1:9083", mapping.getConfig().get("hive.metastore.uris"));

        verify(connectors, times(1)).createHiddenConnector(any(), eq(false));
    }

    @Test
    public void testEmptyCatalogSectionStillRebuildsMappingCatalogs() throws Exception {
        CatalogMgr restored = v3(new CatalogMgr(connectors));
        Map<String, Catalog> catalogs = getField(restored, "catalogs");
        assertTrue(catalogs.isEmpty());

        verify(resources, times(1)).getNeedMappingCatalogResources();
    }

    @Test
    public void testCatalogsAreIndividualEntries() throws Exception {
        CatalogMgr source = new CatalogMgr(connectors);
        for (int i = 0; i < 1024; i++) {
            add(source, catalog(i, "catalog-" + i));
        }
        Path path = LoadImageTestSupport.save(tempDir, source::save);
        CatalogMgr restored = new CatalogMgr(connectors);
        List<IndexEntryPB> sections = LoadImageTestSupport.load(path, restored.getImageV3Loaders());
        assertEquals(1024, sections.get(0).getNumEntries());
        for (int i = 0; i < 1024; i++) {
            GsonEquivalence.assertEquivalent(source.getCatalogByName("catalog-" + i),
                    restored.getCatalogByName("catalog-" + i));
        }

    }
}
