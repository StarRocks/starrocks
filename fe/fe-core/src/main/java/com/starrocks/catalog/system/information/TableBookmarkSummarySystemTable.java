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

import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.epack.catalog.system.SystemIdEPack;
import com.starrocks.lake.bookmark.Bookmark;
import com.starrocks.lake.bookmark.BookmarkManager;
import com.starrocks.lake.bookmark.PhysicalPartitionMeta;
import com.starrocks.lake.bookmark.Reference;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBookmarkReferenceSummary;
import com.starrocks.thrift.TGetTableBookmarkSummaryRequest;
import com.starrocks.thrift.TGetTableBookmarkSummaryResponse;
import com.starrocks.thrift.TLatestChangedPhysicalPartitionEntry;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableBookmarkSummaryInfo;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.TypeFactory;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;

public class TableBookmarkSummarySystemTable {
    public static final String NAME = "table_bookmark_summary";

    public static SystemTable create() {
        // ARRAY<STRUCT<id BIGINT, version BIGINT, time DATETIME>>
        ArrayList<StructField> latestChangedPhysicalPartitionsFields = new ArrayList<>();
        latestChangedPhysicalPartitionsFields.add(new StructField("id", IntegerType.BIGINT));
        latestChangedPhysicalPartitionsFields.add(new StructField("version", IntegerType.BIGINT));
        latestChangedPhysicalPartitionsFields.add(new StructField("time", DateType.DATETIME));
        ArrayType latestChangedType = new ArrayType(new StructType(latestChangedPhysicalPartitionsFields));

        // STRUCT<id VARCHAR, time DATETIME>
        ArrayList<StructField> referenceFields = new ArrayList<>();
        referenceFields.add(new StructField("id", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH)));
        referenceFields.add(new StructField("time", DateType.DATETIME));
        StructType referenceType = new StructType(referenceFields);

        return new SystemTable(SystemIdEPack.TABLE_BOOKMARK_SUMMARY_ID,
                NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("DB_ID", IntegerType.BIGINT)
                        .column("TABLE_ID", IntegerType.BIGINT)
                        .column("BOOKMARK_ID", IntegerType.BIGINT)
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("LOGICAL_PARTITION_COUNT", IntegerType.BIGINT)
                        .column("PHYSICAL_PARTITION_COUNT", IntegerType.BIGINT)
                        .column("REFERENCE_COUNT", IntegerType.BIGINT)
                        .column("LATEST_CHANGED_PHYSICAL_PARTITIONS", latestChangedType)
                        .column("OLDEST_REFERENCE", referenceType)
                        .column("NEWEST_REFERENCE", referenceType)
                        .build(),
                TSchemaTableType.SCH_TABLE_BOOKMARK_SUMMARY);
    }

    public static TGetTableBookmarkSummaryResponse query(TGetTableBookmarkSummaryRequest request) throws TException {
        TGetTableBookmarkSummaryResponse resp = new TGetTableBookmarkSummaryResponse();
        List<TTableBookmarkSummaryInfo> out = new ArrayList<>();

        Optional<Long> dbIdFilter = request.isSetDb_id() ? Optional.of(request.getDb_id()) : Optional.empty();
        Optional<Long> tableIdFilter = request.isSetTable_id() ? Optional.of(request.getTable_id()) : Optional.empty();
        Optional<Long> bookmarkIdFilter =
                request.isSetBookmark_id() ? Optional.of(request.getBookmark_id()) : Optional.empty();
        Set<String> selected = request.isSetSelected_columns()
                ? new HashSet<>(request.getSelected_columns()) : null;

        BookmarkManager mgr = GlobalStateMgr.getCurrentState().getBookmarkManager();
        List<Bookmark.View> views = mgr.listAllBookmarks(dbIdFilter, tableIdFilter, bookmarkIdFilter);

        BookmarkTableAccessFilter authzFilter = new BookmarkTableAccessFilter(request.getAuth_info());
        for (Bookmark.View s : views) {
            Bookmark b = s.getBookmark();
            if (!authzFilter.isAuthorized(b.getDbId(), b.getTableId())) {
                continue;
            }
            TTableBookmarkSummaryInfo info = new TTableBookmarkSummaryInfo();
            fillSummaryColumns(info, s, selected);
            out.add(info);
        }
        resp.table_bookmark_summary_infos = out;
        return resp;
    }

    private static void fillSummaryColumns(TTableBookmarkSummaryInfo info, Bookmark.View s, Set<String> selected) {
        Bookmark b = s.getBookmark();
        info.setDb_id(b.getDbId());
        info.setTable_id(b.getTableId());
        info.setBookmark_id(b.getBookmarkId());
        info.setCreate_time(b.getBookmarkTimeMs());
        info.setLogical_partition_count(b.getLogicalPartitionCount());
        info.setPhysical_partition_count(b.getPhysicalPartitionCount());
        info.setReference_count(s.getReferences().size());

        if (selected == null || selected.contains("LATEST_CHANGED_PHYSICAL_PARTITIONS")) {
            info.setLatest_changed_physical_partitions(top3LatestChanged(b));
        }
        if (selected == null || selected.contains("OLDEST_REFERENCE")) {
            info.setOldest_reference(oldestRef(s.getReferences()));
        }
        if (selected == null || selected.contains("NEWEST_REFERENCE")) {
            info.setNewest_reference(newestRef(s.getReferences()));
        }
    }

    private static List<TLatestChangedPhysicalPartitionEntry> top3LatestChanged(Bookmark b) {
        // Flatten all PhysicalPartitionMeta entries, sort by visibleVersionTimeMs DESC,
        // tie-break by largest physicalPartitionId, take top 3.
        List<long[]> all = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> e : b.getPartitionsMeta().entrySet()) {
            for (Map.Entry<Long, PhysicalPartitionMeta> p : e.getValue().entrySet()) {
                all.add(new long[] {
                        p.getKey(),
                        p.getValue().getVisibleVersion(),
                        p.getValue().getVisibleVersionTimeMs()
                });
            }
        }
        all.sort((x, y) -> {
            int c = Long.compare(y[2], x[2]);
            if (c != 0) {
                return c;
            }
            return Long.compare(y[0], x[0]);
        });
        List<TLatestChangedPhysicalPartitionEntry> top = new ArrayList<>();
        for (int i = 0; i < Math.min(3, all.size()); i++) {
            TLatestChangedPhysicalPartitionEntry entry = new TLatestChangedPhysicalPartitionEntry();
            entry.setId(all.get(i)[0]);
            entry.setVersion(all.get(i)[1]);
            entry.setTime(all.get(i)[2]);
            top.add(entry);
        }
        return top;
    }

    private static TBookmarkReferenceSummary oldestRef(List<Reference.View> refs) {
        if (refs.isEmpty()) {
            return null;
        }
        Reference.View best = refs.get(0);
        for (Reference.View r : refs) {
            if (r.getAcquiredAtMs() < best.getAcquiredAtMs()
                    || (r.getAcquiredAtMs() == best.getAcquiredAtMs()
                            && r.getHolderId().compareTo(best.getHolderId()) < 0)) {
                best = r;
            }
        }
        TBookmarkReferenceSummary out = new TBookmarkReferenceSummary();
        out.setId(best.getHolderId());
        out.setTime(best.getAcquiredAtMs());
        return out;
    }

    private static TBookmarkReferenceSummary newestRef(List<Reference.View> refs) {
        if (refs.isEmpty()) {
            return null;
        }
        Reference.View best = refs.get(0);
        for (Reference.View r : refs) {
            if (r.getAcquiredAtMs() > best.getAcquiredAtMs()
                    || (r.getAcquiredAtMs() == best.getAcquiredAtMs()
                            && r.getHolderId().compareTo(best.getHolderId()) < 0)) {
                best = r;
            }
        }
        TBookmarkReferenceSummary out = new TBookmarkReferenceSummary();
        out.setId(best.getHolderId());
        out.setTime(best.getAcquiredAtMs());
        return out;
    }
}
