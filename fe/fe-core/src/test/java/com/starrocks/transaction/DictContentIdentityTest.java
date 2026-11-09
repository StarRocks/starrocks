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

package com.starrocks.transaction;

import com.starrocks.catalog.ColumnId;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.thrift.TCommitRemoteTxnRequest;
import com.starrocks.thrift.TTabletCommitInfo;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DictContentIdentityTest {
    private static TTabletCommitInfo report(List<String> columns, List<Long> identities) {
        TTabletCommitInfo thrift = new TTabletCommitInfo(1, 2);
        thrift.setInvalid_dict_cache_columns(List.of());
        thrift.setValid_dict_cache_columns(columns);
        thrift.setValid_dict_collected_versions(columns.stream().map(column -> 100L).toList());
        if (identities != null) {
            thrift.setValid_dict_content_identities(identities);
        }
        return thrift;
    }

    private static Set<ColumnId> merge(Map<ColumnId, Long> identities, TTabletCommitInfo... reports) {
        Set<ColumnId> invalid = new HashSet<>();
        for (TabletCommitInfo info : TabletCommitInfo.fromThrift(List.of(reports))) {
            invalid.addAll(info.getInvalidDictCacheColumns());
            info.mergeDictContentIdentities(identities, invalid);
        }
        return invalid;
    }

    @Test
    public void testIdentityMatchesColumnRatherThanListPosition() {
        Map<ColumnId, Long> identities = new HashMap<>();
        merge(identities, report(List.of("b", "a"), List.of(-7L, 0L)));
        merge(identities, report(List.of("a", "b"), List.of(0L, -7L)));
        // Secondary replicas without a dict report do not invalidate a valid report.
        new TabletCommitInfo(3, 4).mergeDictContentIdentities(identities, new HashSet<>());
        Assertions.assertEquals(Map.of(ColumnId.create("a"), 0L, ColumnId.create("b"), -7L), identities);
        Assertions.assertEquals(List.of(100L), TabletCommitInfo.fromThrift(
                List.of(report(List.of("a"), List.of(0L)))).get(0).getValidDictCollectedVersions());
    }

    @Test
    public void testLegacyReportsLeaveIdentitiesEmpty() {
        Map<ColumnId, Long> identities = new HashMap<>();
        Set<ColumnId> invalid = merge(identities,
                report(List.of("a"), null), report(List.of("a", "b"), List.of()));
        Assertions.assertTrue(invalid.isEmpty());
        Assertions.assertTrue(identities.isEmpty());
    }

    @Test
    public void testConflictingIdentityCannotBeRestoredByLaterReports() {
        Map<ColumnId, Long> identities = new HashMap<>();
        TTabletCommitInfo first = report(List.of("a"), List.of(900L));
        TTabletCommitInfo second = report(List.of("a"), List.of(901L));
        Assertions.assertEquals(Set.of(ColumnId.create("a")), merge(identities, first, second, first));
    }

    @Test
    public void testMissingReportDoesNotHideKnownConflict() {
        TTabletCommitInfo first = report(List.of("a"), List.of(0L));
        TTabletCommitInfo second = report(List.of("a"), List.of(-7L));
        TTabletCommitInfo legacy = report(List.of("a"), null);
        for (List<TTabletCommitInfo> reports : List.of(
                List.of(legacy, first, second), List.of(first, legacy, second), List.of(first, second, legacy))) {
            Map<ColumnId, Long> identities = new HashMap<>();
            Assertions.assertEquals(Set.of(ColumnId.create("a")),
                    merge(identities, reports.toArray(new TTabletCommitInfo[0])));
        }
    }

    @Test
    public void testRemoteCommitPreservesIdentityAndCollectedVersion() throws Exception {
        TCommitRemoteTxnRequest request = new TCommitRemoteTxnRequest();
        request.setDb_id(10);
        request.setTxn_id(20);
        request.setCommit_infos(List.of(report(List.of("a"), List.of(-7L))));
        TCommitRemoteTxnRequest received = new TCommitRemoteTxnRequest();
        new TDeserializer().deserialize(received, new TSerializer().serialize(request));
        Map<ColumnId, Long> identities = new HashMap<>();
        TabletCommitInfo info = TabletCommitInfo.fromThrift(received.getCommit_infos()).get(0);
        info.mergeDictContentIdentities(identities, new HashSet<>());
        Assertions.assertEquals(Map.of(ColumnId.create("a"), -7L), identities);
        Assertions.assertEquals(List.of(100L), info.getValidDictCollectedVersions());
    }

    @Test
    public void testPartitionLogPreservesIdentityList() {
        PartitionCommitInfo legacy = GsonUtils.GSON.fromJson(
                "{\"partitionId\":1,\"version\":2}", PartitionCommitInfo.class);
        Assertions.assertTrue(legacy.getDictContentIdentities().isEmpty());
        Assertions.assertTrue(new PartitionCommitInfo(legacy).getDictContentIdentities().isEmpty());

        legacy.setDictContentIdentities(List.of());
        PartitionCommitInfo empty = roundTrip(legacy);
        Assertions.assertTrue(empty.getDictContentIdentities().isEmpty());
        Assertions.assertTrue(new PartitionCommitInfo(empty).getDictContentIdentities().isEmpty());

        legacy.setDictContentIdentities(List.of(0L, -7L));
        Assertions.assertEquals(List.of(0L, -7L), roundTrip(legacy).getDictContentIdentities());
        PartitionCommitInfo copy = new PartitionCommitInfo(legacy);
        legacy.getDictContentIdentities().clear();
        Assertions.assertEquals(List.of(0L, -7L), copy.getDictContentIdentities());
    }

    @Test
    public void testPartitionLogPreservesNullIdentityElement() {
        PartitionCommitInfo info = new PartitionCommitInfo(1, 2, 3);
        List<Long> identities = Arrays.asList(0L, null, -7L);
        info.setDictContentIdentities(identities);
        Assertions.assertEquals(identities, roundTrip(info).getDictContentIdentities());
    }

    private static PartitionCommitInfo roundTrip(PartitionCommitInfo info) {
        return GsonUtils.GSON.fromJson(GsonUtils.GSON.toJson(info), PartitionCommitInfo.class);
    }
}
