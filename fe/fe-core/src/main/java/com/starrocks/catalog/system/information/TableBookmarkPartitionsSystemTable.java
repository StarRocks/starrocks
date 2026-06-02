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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetTableBookmarkPartitionsRequest;
import com.starrocks.thrift.TGetTableBookmarkPartitionsResponse;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableBookmarkPartitionInfo;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableBookmarkPartitionsSystemTable {
    public static final String NAME = "table_bookmark_partitions";

    public static SystemTable create() {
        return new SystemTable(SystemIdEPack.TABLE_BOOKMARK_PARTITIONS_ID,
                NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("DB_ID", IntegerType.BIGINT)
                        .column("TABLE_ID", IntegerType.BIGINT)
                        .column("BOOKMARK_ID", IntegerType.BIGINT)
                        .column("LOGICAL_PARTITION_ID", IntegerType.BIGINT)
                        .column("PHYSICAL_PARTITION_ID", IntegerType.BIGINT)
                        .column("VISIBLE_VERSION", IntegerType.BIGINT)
                        .column("VISIBLE_VERSION_TIME", DateType.DATETIME)
                        .column("BASE_MATERIALIZED_INDEX_META_ID", IntegerType.BIGINT)
                        .column("BASE_MATERIALIZED_INDEX_ID", IntegerType.BIGINT)
                        .build(),
                TSchemaTableType.SCH_TABLE_BOOKMARK_PARTITIONS);
    }

    public static TGetTableBookmarkPartitionsResponse query(TGetTableBookmarkPartitionsRequest request)
            throws TException {
        TGetTableBookmarkPartitionsResponse resp = new TGetTableBookmarkPartitionsResponse();
        List<TTableBookmarkPartitionInfo> out = new ArrayList<>();

        Optional<Long> dbIdFilter = request.isSetDb_id() ? Optional.of(request.getDb_id()) : Optional.empty();
        Optional<Long> tableIdFilter = request.isSetTable_id() ? Optional.of(request.getTable_id()) : Optional.empty();
        Optional<Long> bookmarkIdFilter =
                request.isSetBookmark_id() ? Optional.of(request.getBookmark_id()) : Optional.empty();

        BookmarkManager mgr = GlobalStateMgr.getCurrentState().getBookmarkManager();
        List<Bookmark.View> views = mgr.listAllBookmarks(dbIdFilter, tableIdFilter, bookmarkIdFilter);

        BookmarkTableAccessFilter authzFilter = new BookmarkTableAccessFilter(request.getAuth_info());
        for (Bookmark.View s : views) {
            Bookmark b = s.getBookmark();
            if (!authzFilter.isAuthorized(b.getDbId(), b.getTableId())) {
                continue;
            }
            for (Map.Entry<Long, Map<Long, PhysicalPartitionMeta>> lp : b.getPartitionsMeta().entrySet()) {
                for (Map.Entry<Long, PhysicalPartitionMeta> pp : lp.getValue().entrySet()) {
                    TTableBookmarkPartitionInfo info = new TTableBookmarkPartitionInfo();
                    info.setDb_id(b.getDbId());
                    info.setTable_id(b.getTableId());
                    info.setBookmark_id(b.getBookmarkId());
                    info.setLogical_partition_id(lp.getKey());
                    info.setPhysical_partition_id(pp.getKey());
                    info.setVisible_version(pp.getValue().getVisibleVersion());
                    info.setVisible_version_time(pp.getValue().getVisibleVersionTimeMs());
                    info.setBase_materialized_index_meta_id(pp.getValue().getBaseMaterializedIndexMetaId());
                    info.setBase_materialized_index_id(pp.getValue().getBaseMaterializedIndexId());
                    out.add(info);
                }
            }
        }
        resp.table_bookmark_partition_infos = out;
        return resp;
    }
}
