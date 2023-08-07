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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/journal/bdbje/BDBTool.java

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

package com.starrocks.journal.bdbje;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.starrocks.journal.JournalEntity;
import com.starrocks.meta.MetaContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;

public class BDBTool {
    private static final Logger LOG = LogManager.getLogger(BDBTool.class);

    private String metaPath;
    private BDBToolOptions options;

    public BDBTool(String metaPath, BDBToolOptions options) {
        this.metaPath = metaPath;
        this.options = options;
    }

    public boolean run() {
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(false);
        envConfig.setReadOnly(true);
        envConfig.setCachePercent(20);

        Environment env = null;
        try {
            env = new Environment(new File(metaPath), envConfig);
        } catch (DatabaseException e) {
            LOG.warn(e);
            System.err.println("Failed to open BDBJE env: " + metaPath + ". exit");
            return false;
        }
        Preconditions.checkNotNull(env);

        try {
            if (options.isListDbs()) {
                // list all databases
                List<String> dbNames = env.getDatabaseNames();
                JSONArray jsonArray = new JSONArray(dbNames);
                System.out.println(jsonArray.toString());
                return true;
            } else {
                // db operations
                String dbName = options.getDbName();
                Preconditions.checkState(!Strings.isNullOrEmpty(dbName));
                DatabaseConfig dbConfig = new DatabaseConfig();
                dbConfig.setAllowCreate(false);
                dbConfig.setReadOnly(true);
                try (Database db = env.openDatabase(null, dbName, dbConfig)) {
                    if (options.isDbStat()) {
                        // get db stat
                        Map<String, String> statMap = Maps.newHashMap();
                        statMap.put("count", String.valueOf(db.count()));
                        JSONObject jsonObject = new JSONObject(statMap);
                        System.out.println(jsonObject.toString());
                        return true;
                    } else {
                        // set from key
                        long fromKey = 0L;
                        String fromKeyStr = options.hasFromKey() ? options.getFromKey() : dbName;
                        try {
                            fromKey = Long.parseLong(fromKeyStr);
                        } catch (NumberFormatException e) {
                            System.err.println("Not a valid from key: " + fromKeyStr);
                            return false;
                        }

                        // set end key
                        long endKey = fromKey + db.count() - 1;
                        if (options.hasEndKey()) {
                            try {
                                endKey = Long.parseLong(options.getEndKey());
                            } catch (NumberFormatException e) {
                                System.err.println("Not a valid end key: " + options.getEndKey());
                                return false;
                            }
                        }

                        if (fromKey > endKey) {
                            System.err.println("from key should less than or equal to end key["
                                    + fromKey + " vs. " + endKey + "]");
                            return false;
                        }

                        // meta version
                        MetaContext metaContext = new MetaContext();
                        metaContext.setStarRocksMetaVersion(options.getStarRocksMetaVersion());
                        metaContext.setThreadLocalInfo();

                        for (long key = fromKey; key <= endKey; key++) {
                            getValueByKey(db, key);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn(e);
            System.err.println("Failed to run bdb tools");
            return false;
        }
        return true;
    }

    private void getValueByKey(Database db, Long key) {

        DatabaseEntry queryKey = new DatabaseEntry();
        TupleBinding<Long> myBinding = TupleBinding.getPrimitiveBinding(Long.class);
        myBinding.objectToEntry(key, queryKey);
        DatabaseEntry value = new DatabaseEntry();

        OperationStatus status = db.get(null, queryKey, value, LockMode.READ_COMMITTED);
        if (status == OperationStatus.SUCCESS) {
            byte[] retData = value.getData();
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(retData));
            JournalEntity entity = new JournalEntity();
            try {
                entity.readFields(in);
            } catch (Exception e) {
                LOG.warn(e);
                System.err.println("Fail to read journal entity for key: " + key + ". reason: " + e.getMessage());
                System.exit(-1);
            }
            System.out.println("key: " + key);
            System.out.println("op code: " + entity.getOpCode());
            System.out.println("value: " + entity.getData().toString());
        } else if (status == OperationStatus.NOTFOUND) {
            System.out.println("key: " + key);
            System.out.println("value: NOT FOUND");
        }
    }
}
