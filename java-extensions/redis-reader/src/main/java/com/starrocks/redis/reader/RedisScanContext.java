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

package com.starrocks.redis.reader;

import java.util.ArrayList;
import java.util.List;

public class RedisScanContext {
    public String dbName;
    public String tblName;
    private String user;
    public String redisURL;
    public String password;
    public String valueDataFormat;
    public List<String> columnsNames = new ArrayList<>();
    public List<String> columnsTypes = new ArrayList<>();

    private int fetchSize;

    public RedisScanContext() {}
    public RedisScanContext(String dbName, String tblName, String redisURL, String user, String password,
                            String valueDataFormat, int fetchSize, List<String> columnsName, List<String> columnsType) {
        this.dbName = dbName;
        this.tblName = tblName;
        this.redisURL = redisURL;
        this.user = user;
        this.password = password;
        this.fetchSize = fetchSize;
        this.valueDataFormat = valueDataFormat;
        this.columnsNames = columnsName;
        this.columnsTypes = columnsType;
    }
}
