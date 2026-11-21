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

import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupClassifier.CostRange;
import com.starrocks.catalog.ResourceGroupClassifier.QueryType;
import com.starrocks.persist.proto.ResourceGroupClassifierPB;
import com.starrocks.persist.proto.ResourceGroupCostRangePB;
import com.starrocks.persist.proto.ResourceGroupPB;
import com.starrocks.persist.proto.ResourceGroupQueryTypesPB;
import com.starrocks.thrift.TWorkGroupType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.starrocks.common.jmockit.Deencapsulation.getField;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResourceGroupProtoTest {
    static ResourceGroupClassifier classifier(long groupId) {
        ResourceGroupClassifier classifier = new ResourceGroupClassifier();
        classifier.setId(groupId + 100);
        classifier.setResourceGroupId(groupId);
        classifier.setUser("alice");
        classifier.setRole("analyst");
        classifier.setSourceIp("10.0.0.0/24");
        classifier.setQueryTypes(new HashSet<>(Set.of(QueryType.SELECT, QueryType.INSERT)));
        classifier.setDatabases(List.of(10L, 20L));
        classifier.setPlanCpuCostRange(CostRange.fromString("[1.234567891, 10.987654321)"));
        classifier.setPlanMemCostRange(CostRange.fromString("[100, 1000)"));
        return classifier;
    }

    static ResourceGroup group(long id, long version) {
        ResourceGroup group = new ResourceGroup();
        group.setId(id);
        group.setName("rg" + id);
        group.setVersion(version);
        group.setCpuWeight(7);
        group.setCpuWeightPercent(13);
        group.setExclusiveCpuCores(3);
        group.setExclusiveCpuPercent(17);
        group.setMaxCpuCores(19);
        group.setMemLimit(0.37);
        group.setMemPool("pool");
        group.setBigQueryMemLimit(123456789);
        group.setBigQueryScanRowsLimit(987654321);
        group.setBigQueryCpuSecondLimit(31);
        group.setConcurrencyLimit(11);
        group.setSpillMemLimitThreshold(0.67);
        group.setMemUsedPctLimit(0.81);
        group.setResourceGroupType(TWorkGroupType.WG_NORMAL);
        group.setWarehouses(new ArrayList<>(List.of("warehouse1", "warehouse2")));
        group.setClassifiers(new ArrayList<>(List.of(classifier(id))));
        return group;
    }

    @Test
    public void testAllPersistedFields() throws Exception {
        ResourceGroup source = group(2, 31);
        ResourceGroup loaded = ResourceGroup.fromProto(ResourceGroupPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, ResourceGroup.class), loaded);
        assertEquals(7, loaded.getRawCpuWeight());
        assertEquals(13, loaded.getCpuWeightPercent());
        assertEquals(3, loaded.getExclusiveCpuCores());
        assertEquals(17, loaded.getExclusiveCpuPercent());
        assertEquals(source.toThrift(), loaded.toThrift());
        assertEquals(source.getClassifiers().get(0).weight(), loaded.getClassifiers().get(0).weight());
    }

    @Test
    public void testNullAndZeroRemainDistinct() {
        ResourceGroup source = new ResourceGroup();
        ResourceGroup loaded = ResourceGroup.fromProto(source.toProto());
        GsonEquivalence.assertEquivalent(source, loaded);
        assertNull(loaded.getRawCpuWeight());
        assertNull(loaded.getExclusiveCpuCores());
        assertNull(loaded.getClassifiers());
        source.setCpuWeight(0);
        source.setExclusiveCpuCores(0);
        source.setClassifiers(new ArrayList<>());
        source.setWarehouses(new ArrayList<>());
        loaded = ResourceGroup.fromProto(source.toProto());
        assertEquals(0, loaded.getRawCpuWeight());
        assertEquals(0, loaded.getExclusiveCpuCores());
        assertTrue(loaded.getClassifiers().isEmpty());
        assertTrue(loaded.getWarehouses().isEmpty());
    }

    @Test
    public void testEveryGroupType() {
        for (TWorkGroupType type : TWorkGroupType.values()) {
            ResourceGroup source = group(2, 3);
            source.setResourceGroupType(type);
            assertEquals(type, ResourceGroup.fromProto(source.toProto()).getResourceGroupType());
        }
    }

    @Test
    public void testClassifierMatching() throws Exception {
        ResourceGroupClassifier source = classifier(2);
        ResourceGroupClassifier loaded = ResourceGroupClassifier.fromProto(
                ResourceGroupClassifierPB.parseFrom(source.toProto().toByteArray()));
        GsonEquivalence.assertEquivalent(StatisticsSecurityTestSupport.gson(source, ResourceGroupClassifier.class), loaded);
        assertTrue(matches(loaded, "alice", "analyst", QueryType.SELECT, "10.0.0.9", Set.of(10L), 2, 200));
        assertFalse(matches(loaded, "bob", "analyst", QueryType.SELECT, "10.0.0.9", Set.of(10L), 2, 200));
        assertFalse(matches(loaded, "alice", "other", QueryType.SELECT, "10.0.0.9", Set.of(10L), 2, 200));
        assertFalse(matches(loaded, "alice", "analyst", QueryType.DELETE, "10.0.0.9", Set.of(10L), 2, 200));
        assertFalse(matches(loaded, "alice", "analyst", QueryType.SELECT, "10.0.1.9", Set.of(10L), 2, 200));
        assertFalse(matches(loaded, "alice", "analyst", QueryType.SELECT, "10.0.0.9", Set.of(30L), 2, 200));
        assertFalse(matches(loaded, "alice", "analyst", QueryType.SELECT, "10.0.0.9", Set.of(10L), 0, 200));
        assertFalse(matches(loaded, "alice", "analyst", QueryType.SELECT, "10.0.0.9", Set.of(10L), 2, 1000));
        assertFalse(loaded.isSatisfied(Set.of(999L), "alice", List.of("analyst"), QueryType.SELECT,
                "10.0.0.9", Set.of(10L), 2, 200));
    }

    private boolean matches(ResourceGroupClassifier classifier, String user, String role, QueryType type,
                            String ip, Set<Long> dbs, double cpu, double mem) {
        return classifier.isSatisfied(Set.of(2L), user, List.of(role), type, ip, dbs, cpu, mem);
    }

    @Test
    public void testCostRangeKeepsExactDoubleBoundaries() {
        ResourceGroupClassifier loaded = ResourceGroupClassifier.fromProto(classifier(2).toProto());
        CostRange range = loaded.getPlanCpuCostRange();
        assertFalse(range.contains(Math.nextDown(1.234567891)));
        assertTrue(range.contains(1.234567891));
        assertTrue(range.contains(Math.nextDown(10.987654321)));
        assertFalse(range.contains(10.987654321));
        assertEquals(1.234567891, (double) getField(range, "min"));
        assertEquals(10.987654321, (double) getField(range, "max"));
    }

    @Test
    public void testEveryQueryType() {
        ResourceGroupClassifier source = classifier(2);
        source.setQueryTypes(new HashSet<>(Arrays.asList(QueryType.values())));
        assertEquals(source.getQueryTypes(), ResourceGroupClassifier.fromProto(source.toProto()).getQueryTypes());
    }

    @Test
    public void testClassifierNullAndEmptyCollections() {
        ResourceGroupClassifier source = new ResourceGroupClassifier();
        GsonEquivalence.assertEquivalent(source, ResourceGroupClassifier.fromProto(source.toProto()));
        source.setQueryTypes(new HashSet<>());
        source.setDatabases(List.of());
        ResourceGroupClassifier loaded = ResourceGroupClassifier.fromProto(source.toProto());
        assertNotNull(loaded.getQueryTypes());
        assertTrue(loaded.getQueryTypes().isEmpty());
        assertTrue(loaded.getDatabases().isEmpty());
    }

    @Test
    public void testRestoredCollectionsAreIndependent() {
        ResourceGroup source = group(2, 3);
        ResourceGroup loaded = ResourceGroup.fromProto(source.toProto());
        loaded.getWarehouses().clear();
        loaded.getClassifiers().get(0).getQueryTypes().clear();
        loaded.getClassifiers().get(0).getDatabases().clear();
        loaded.getClassifiers().clear();
        assertEquals(2, source.getWarehouses().size());
        assertEquals(1, source.getClassifiers().size());
        assertEquals(2, source.getClassifiers().get(0).getQueryTypes().size());
        assertEquals(2, source.getClassifiers().get(0).getDatabases().size());
    }

    @Test
    public void testUnknownFieldsAndMissingDefaults() {
        ResourceGroup source = group(2, 3);
        GsonEquivalence.assertEquivalent(source, ResourceGroup.fromProto(source.toProto().toBuilder()
                .setUnknownFields(StatisticsSecurityTestSupport.unknownFields()).build()));
        GsonEquivalence.assertEquivalent(new ResourceGroup(), ResourceGroup.fromProto(ResourceGroupPB.getDefaultInstance()));
    }

    @Test
    public void testInvalidKindsAndRanges() {
        assertThrows(IllegalArgumentException.class, () -> ResourceGroup.fromProto(
                group(2, 3).toProto().toBuilder().setResourceGroupTypeValue(999).build()));
        assertThrows(IllegalArgumentException.class, () -> ResourceGroupClassifier.fromProto(
                classifier(2).toProto().toBuilder().setQueryTypes(
                        ResourceGroupQueryTypesPB.newBuilder().addValuesValue(999)).build()));
        for (ResourceGroupCostRangePB invalid : List.of(ResourceGroupCostRangePB.getDefaultInstance(),
                ResourceGroupCostRangePB.newBuilder().setMin(5).setMax(5).build(),
                ResourceGroupCostRangePB.newBuilder().setMin(6).setMax(5).build(),
                ResourceGroupCostRangePB.newBuilder().setMin(Double.NaN).setMax(5).build(),
                ResourceGroupCostRangePB.newBuilder().setMin(0).setMax(Double.POSITIVE_INFINITY).build())) {
            assertThrows(IllegalArgumentException.class, () -> ResourceGroupClassifier.fromProto(
                    classifier(2).toProto().toBuilder().setPlanCpuCostRange(invalid).build()));
        }
    }
}
