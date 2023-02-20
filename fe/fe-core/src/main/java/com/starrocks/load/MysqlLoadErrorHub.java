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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/MysqlLoadErrorHub.java

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

package com.starrocks.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.thrift.TMysqlErrorHubInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class MysqlLoadErrorHub extends LoadErrorHub {
    private static final Logger LOG = LogManager.getLogger(MysqlLoadErrorHub.class);

    private static final String QUERY_SQL_FIRST = "SELECT job_id, error_msg FROM ";
    private static final String QUERY_SQL_LAST = " WHERE job_id = ? LIMIT ? ";
    private static final long MAX_LINE = 10;
    private static final int STMT_TIMEOUT_S = 5;

    private MysqlParam param;

    public static class MysqlParam implements Writable {
        private String host;
        private int port;
        private String user;
        private String passwd;
        private String db;
        private String table;

        public MysqlParam() {
            host = "";
            port = 0;
            user = "";
            passwd = "";
            db = "";
            table = "";
        }

        public MysqlParam(String host, int port, String user, String passwd, String db, String table) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.passwd = passwd;
            this.db = db;
            this.table = table;
        }

        public String getBrief() {
            Map<String, String> briefMap = Maps.newHashMap();
            briefMap.put("host", host);
            briefMap.put("port", String.valueOf(port));
            briefMap.put("user", user);
            briefMap.put("password", passwd);
            briefMap.put("database", db);
            briefMap.put("table", table);
            PrintableMap<String, String> printableMap = new PrintableMap<>(briefMap, "=", true, false, true);
            return printableMap.toString();
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getUser() {
            return user;
        }

        public String getPasswd() {
            return passwd;
        }

        public String getDb() {
            return db;
        }

        public String getTable() {
            return table;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, host);
            out.writeInt(port);
            Text.writeString(out, user);
            Text.writeString(out, passwd);
            Text.writeString(out, db);
            Text.writeString(out, table);
        }

        public void readFields(DataInput in) throws IOException {
            host = Text.readString(in);
            port = in.readInt();
            user = Text.readString(in);
            passwd = Text.readString(in);
            db = Text.readString(in);
            table = Text.readString(in);
        }

        public TMysqlErrorHubInfo toThrift() {
            TMysqlErrorHubInfo info = new TMysqlErrorHubInfo(host, port, user, passwd, db, table);
            return info;
        }
    }

    public MysqlLoadErrorHub(MysqlParam mysqlParam) {
        Preconditions.checkNotNull(mysqlParam);
        param = mysqlParam;
    }

    @Override
    public boolean prepare() {
        return true;
    }

    @Override
    public boolean close() {
        return true;
    }
}
