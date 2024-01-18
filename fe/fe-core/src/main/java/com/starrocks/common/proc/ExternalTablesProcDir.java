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

package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.util.ProcResultUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;

import java.util.ArrayList;
import java.util.List;

public class ExternalTablesProcDir implements ProcDirInterface {

    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("TableName")
            .build();

    private String catalogName;
    private String dbName;

    public ExternalTablesProcDir(String catalogName, String dbName) {
        this.catalogName = catalogName;
        this.dbName = dbName;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) throws AnalysisException {
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("name is null");
        }
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Database db = metadataMgr.getDb(catalogName, dbName);
        if (db == null) {
            throw new AnalysisException("db: " + dbName + " not exists");
        }
        Table tbl = metadataMgr.getTable(catalogName, dbName, name);
        if (tbl == null) {
            throw new AnalysisException("table : " + name + " not exists");
        }
        return new ExternalTableProcDir(tbl);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Preconditions.checkNotNull(metadataMgr);
        List<String> tables = null;
        tables = metadataMgr.listTableNames(catalogName, dbName);

        // get info
        List<List<Comparable>> tableInfos = new ArrayList<List<Comparable>>();
        for (String tableName : tables) {
            List<Comparable> tableInfo = new ArrayList<Comparable>();
            tableInfo.add(tableName);
            tableInfos.add(tableInfo);
        }
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        ProcResultUtils.convertToMetaResult(result, tableInfos);
        return result;
    }
}
