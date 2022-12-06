// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.util.ProcResultUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ExternalDbsProcDir implements ProcDirInterface {
    public static final ImmutableList<String> TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("DbName")
            .build();
    private String catalogName;

    public ExternalDbsProcDir(String catalogName) {
        this.catalogName = catalogName;
    }

    @Override
    public ProcResult fetchResult() {
        MetadataMgr metadataMgr = GlobalStateMgr.getCurrentState().getMetadataMgr();
        Preconditions.checkNotNull(metadataMgr);
        BaseProcResult result = new BaseProcResult();
        result.setNames(TITLE_NAMES);

        List<String> dbNames;
        dbNames = metadataMgr.listDbNames(catalogName);

        if (dbNames == null || dbNames.isEmpty()) {
            // empty
            return result;
        }

        List<List<Comparable>> dbInfos = new ArrayList<>();


        if (Config.enable_check_db_state) {
            // if a large number of db under the catalog, this operation is very slow
            dbNames = dbNames.stream().filter(dbName -> {
                Database db = metadataMgr.getDb(catalogName, dbName);
                return db != null;
            }).collect(Collectors.toList());
        }

        for (String dbName : dbNames) {
            List<Comparable> dbInfo = new ArrayList<>();
            dbInfo.add(dbName);
            dbInfos.add(dbInfo);
        }
        ProcResultUtils.convertToMetaResult(result, dbInfos);
        return result;
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        return false;
    }

    @Override
    public ProcNodeInterface lookup(String name) {
        Database db = GlobalStateMgr.getCurrentState().getMetadataMgr().getDb(catalogName, name);
        if (db == null) {
            throw new SemanticException("Database[" + name + "] does not exist.");
        }
        return new ExternalTablesProcDir(catalogName, name);
    }
}
