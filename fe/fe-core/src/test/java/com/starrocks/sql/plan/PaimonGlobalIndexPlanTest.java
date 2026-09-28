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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Table;
import com.starrocks.common.DdlException;
import com.starrocks.common.tvr.TvrVersionRange;
import com.starrocks.connector.CatalogConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.index.ConnectorIndexCoverage;
import com.starrocks.connector.index.ConnectorIndexDescriptor;
import com.starrocks.connector.index.ConnectorIndexMetadata;
import com.starrocks.connector.index.ConnectorIndexOperation;
import com.starrocks.connector.index.ConnectorIndexTableType;
import com.starrocks.connector.index.ConnectorIndexType;
import com.starrocks.connector.index.VectorIndexMetric;
import com.starrocks.connector.informationschema.InformationSchemaMetadata;
import com.starrocks.connector.metadata.TableMetaMetadata;
import com.starrocks.connector.paimon.PaimonMetadata;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class PaimonGlobalIndexPlanTest extends ConnectorPlanTestBase {
    private static PaimonMetadata indexedMetadata;
    private static AtomicInteger indexMetadataLoads;

    @BeforeAll
    public static void installIndexMetadataWrapper() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        PaimonMetadata paimonMetadata = (PaimonMetadata) metadataMgr
                .getOptionalMetadata(MOCK_PAIMON_CATALOG_NAME).orElseThrow();
        indexedMetadata = spy(paimonMetadata);
        indexMetadataLoads = new AtomicInteger();
        doAnswer(invocation -> {
            indexMetadataLoads.incrementAndGet();
            Table table = invocation.getArgument(0);
            TvrVersionRange versionRange = invocation.getArgument(1);
            long snapshotId = versionRange == null ? ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID
                    : versionRange.end().orElse(ConnectorIndexMetadata.UNKNOWN_SNAPSHOT_ID);
            if ("vector_table".equals(table.getName())) {
                ConnectorIndexDescriptor descriptor = new ConnectorIndexDescriptor(
                        ConnectorIndexType.VECTOR, "lumina", 1, "embedding", false, false,
                        Map.of(ConnectorIndexDescriptor.OPTION_METRIC, VectorIndexMetric.L2.name(),
                                ConnectorIndexDescriptor.OPTION_DIMENSION, "2"),
                        Set.of(ConnectorIndexOperation.VECTOR_TOP_N), ConnectorIndexCoverage.UNKNOWN);
                return ConnectorIndexMetadata.of(snapshotId, ConnectorIndexTableType.DATA_EVOLUTION,
                        List.of(descriptor), Map.of("pk", 0, "embedding", 1), Set.of());
            }
            ConnectorIndexDescriptor descriptor = new ConnectorIndexDescriptor(
                    ConnectorIndexType.BITMAP, "bitmap", 0, "pk", Map.of(),
                    Set.of(ConnectorIndexOperation.EQUAL), ConnectorIndexCoverage.UNKNOWN);
            return ConnectorIndexMetadata.of(snapshotId, ConnectorIndexTableType.DATA_EVOLUTION,
                    List.of(descriptor));
        }).when(indexedMetadata).getIndexMetadata(any(Table.class), any(TvrVersionRange.class));

        // Match the production connector path: MetadataMgr returns CatalogConnectorMetadata,
        // which must delegate index metadata to the Paimon provider selected for the table.
        metadataMgr.registerMockedMetadata(MOCK_PAIMON_CATALOG_NAME, new CatalogConnectorMetadata(
                indexedMetadata,
                new InformationSchemaMetadata(MOCK_PAIMON_CATALOG_NAME),
                new TableMetaMetadata(MOCK_PAIMON_CATALOG_NAME, "paimon")));
        try {
            connectContext.changeCatalogDb(MOCK_PAIMON_CATALOG_NAME + ".pmn_db1");
        } catch (DdlException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeEach
    public void resetIndexMetadataTracking() {
        clearInvocations(indexedMetadata);
        indexMetadataLoads.set(0);
    }

    @Test
    public void testPredicateIndexPlanningThroughMetadataMgr() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM unpartitioned_table WHERE pk = '1'");

        assertContains(plan, "index[");
        verify(indexedMetadata, atLeastOnce()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertTrue(indexMetadataLoads.get() > 0);
    }

    @Test
    public void testUnsupportedPredicateSkipsMetadataIo() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM unpartitioned_table WHERE length(pk) = 1");

        Assertions.assertFalse(plan.contains("index["));
        verify(indexedMetadata, never()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertEquals(0, indexMetadataLoads.get());
    }

    @Test
    public void testVectorTopNPlanningThroughMetadataMgr() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk, approx_l2_distance([1.0, 2.0], embedding) AS score "
                        + "FROM vector_table ORDER BY score ASC LIMIT 10");

        assertContains(plan, "index[");
        assertContains(plan, "metric=L2");
        assertContains(plan, "k=10");
        assertContains(plan, "nullsFirst=true");
        assertContains(plan, "score=approx_l2_distance");
        verify(indexedMetadata, atLeastOnce()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertTrue(indexMetadataLoads.get() > 0);
    }

    @Test
    public void testVectorRankWindowIsNotAnnotated() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk FROM (SELECT pk, RANK() OVER (ORDER BY "
                        + "approx_l2_distance([1.0, 2.0], embedding)) AS ranking "
                        + "FROM vector_table) ranked WHERE ranking <= 10");

        Assertions.assertFalse(plan.contains("index["));
        verify(indexedMetadata, never()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertEquals(0, indexMetadataLoads.get());
    }

    @Test
    public void testVectorMetricMismatchIsNotAnnotated() throws Exception {
        String plan = getLogicalFragmentPlan(
                "SELECT pk, approx_cosine_similarity([1.0, 2.0], embedding) AS score "
                        + "FROM vector_table ORDER BY score DESC LIMIT 10");

        Assertions.assertFalse(plan.contains("index["));
        verify(indexedMetadata, atLeastOnce()).getIndexMetadata(
                any(Table.class), any(TvrVersionRange.class));
        Assertions.assertTrue(indexMetadataLoads.get() > 0);
    }
}
