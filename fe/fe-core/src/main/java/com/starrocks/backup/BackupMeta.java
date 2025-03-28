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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/backup/BackupMeta.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.backup;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.Table;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BackupMeta implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(BackupMeta.class);

    // tbl name -> tbl
    @SerializedName(value = "tblNameMap")
    private Map<String, Table> tblNameMap = Maps.newHashMap();
    // tbl id -> tbl
    private Map<Long, Table> tblIdMap = Maps.newHashMap();
    @SerializedName(value = "functions")
    private List<Function> functions = Lists.newArrayList();
    @SerializedName(value = "catalogs")
    private List<Catalog> catalogs = Lists.newArrayList();

    private BackupMeta() {

    }

    public BackupMeta(List<Table> tables) {
        for (Table table : tables) {
            tblNameMap.put(table.getName(), table);
            tblIdMap.put(table.getId(), table);
        }
    }

    public Map<String, Table> getTables() {
        return tblNameMap;
    }

    public Table getTable(String tblName) {
        return tblNameMap.get(tblName);
    }

    public Table getTable(Long tblId) {
        return tblIdMap.get(tblId);
    }

    public void setFunctions(List<Function> functions) {
        this.functions = functions;
    }

    public List<Function> getFunctions() {
        return functions;
    }

    public void setCatalogs(List<Catalog> catalogs) {
        this.catalogs = catalogs;
    }

    public List<Catalog> getCatalogs() {
        return catalogs;
    }

    public static BackupMeta fromFile(String filePath, int starrocksMetaVersion) throws IOException {
        File file = new File(filePath);
        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            BackupMeta backupMeta = BackupMeta.read(dis);
            return backupMeta;
        }
    }

    public void writeToFile(File metaInfoFile) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(metaInfoFile));
        try {
            write(dos);
            dos.flush();
        } finally {
            dos.close();
        }
    }

    public boolean compatibleWith(BackupMeta other) {
        // TODO
        return false;
    }

    public static BackupMeta read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), BackupMeta.class);
    }



    @Override
    public void gsonPostProcess() throws IOException {
        tblIdMap = Maps.newHashMap();
        for (Map.Entry<String, Table> entry : tblNameMap.entrySet()) {
            tblIdMap.put(entry.getValue().getId(), entry.getValue());
        }
    }
}
