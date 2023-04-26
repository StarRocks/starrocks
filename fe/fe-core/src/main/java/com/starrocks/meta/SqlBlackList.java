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


package com.starrocks.meta;

import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

// Used by sql's blacklist
public class SqlBlackList {
    private static final SqlBlackList INSTANCE = new SqlBlackList();

    public static SqlBlackList getInstance() {
        return INSTANCE;
    }

    public static void verifying(String sql) throws AnalysisException {
        for (BlackListSql patternAndId : getInstance().sqlBlackListMap.values()) {
            Matcher m = patternAndId.pattern.matcher(sql);
            if (m.find()) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_SQL_IN_BLACKLIST_ERROR);
            }
        }
    }

    // we use string of sql as key, and (pattern, id) as value.
    public void put(Pattern pattern) {
        if (!sqlBlackListMap.containsKey(pattern.toString())) {
            long id = ids.getAndIncrement();
            sqlBlackListMap.putIfAbsent(pattern.toString(), new BlackListSql(pattern, id));
        }
    }

    // we delete sql's regular expression use id, so we iterate this map.
    public void delete(long id) {
        for (Map.Entry<String, BlackListSql> entry : sqlBlackListMap.entrySet()) {
            if (entry.getValue().id == id) {
                sqlBlackListMap.remove(entry.getKey());
            }
        }
    }

    // sqlBlackListMap: key is String(sql), value is BlackListSql.
    // BlackListSql is (Pattern, id). Pattern is the regular expression, id marks this sql, and is show with "show sqlblacklist";
    public ConcurrentMap<String, BlackListSql> sqlBlackListMap = new ConcurrentHashMap<>();

    // ids used in sql blacklist
    public AtomicLong ids = new AtomicLong();
}

