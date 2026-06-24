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
import com.starrocks.lake.bookmark.Reference;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TGetTableBookmarkReferencesRequest;
import com.starrocks.thrift.TGetTableBookmarkReferencesResponse;
import com.starrocks.thrift.TSchemaTableType;
import com.starrocks.thrift.TTableBookmarkReferenceInfo;
import com.starrocks.type.DateType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.TypeFactory;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.starrocks.catalog.system.SystemTable.MAX_FIELD_VARCHAR_LENGTH;

public class TableBookmarkReferencesSystemTable {
    public static final String NAME = "table_bookmark_references";

    public static SystemTable create() {
        return new SystemTable(SystemIdEPack.TABLE_BOOKMARK_REFERENCES_ID,
                NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("DB_ID", IntegerType.BIGINT)
                        .column("TABLE_ID", IntegerType.BIGINT)
                        .column("BOOKMARK_ID", IntegerType.BIGINT)
                        .column("HOLDER_ID", TypeFactory.createVarcharType(MAX_FIELD_VARCHAR_LENGTH))
                        .column("CREATE_TIME", DateType.DATETIME)
                        .column("TTL_MS", IntegerType.BIGINT)
                        .build(),
                TSchemaTableType.SCH_TABLE_BOOKMARK_REFERENCES);
    }

    public static TGetTableBookmarkReferencesResponse query(TGetTableBookmarkReferencesRequest request)
            throws TException {
        TGetTableBookmarkReferencesResponse resp = new TGetTableBookmarkReferencesResponse();
        List<TTableBookmarkReferenceInfo> out = new ArrayList<>();

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
            for (Reference.View ref : s.getReferences()) {
                TTableBookmarkReferenceInfo info = new TTableBookmarkReferenceInfo();
                info.setDb_id(b.getDbId());
                info.setTable_id(b.getTableId());
                info.setBookmark_id(b.getBookmarkId());
                info.setHolder_id(ref.getHolderId());
                info.setCreate_time(ref.getAcquiredAtMs());
                info.setTtl(ref.getTtlMs());
                out.add(info);
            }
        }
        resp.table_bookmark_reference_infos = out;
        return resp;
    }
}
