// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.common.proc;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.util.ProcResultUtils;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;

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
        try {
            tables = metadataMgr.listTableNames(catalogName, dbName);
        } catch (DdlException e) {
            throw new SemanticException(String.format("Get external tables error: %s", e.getMessage()));
        }
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
