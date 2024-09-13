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

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Environment;

public class BDBDatabase {
    public static Database openDatabase(Environment environment, String db) {
        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setTransactional(true);
        databaseConfig.setAllowCreate(true);
        databaseConfig.setReadOnly(false);
        databaseConfig.setSortedDuplicatesVoid(false);
        return environment.openDatabase(null, db, databaseConfig);
    }

    public static void truncateDatabase(Environment environment, String db) {
        environment.truncateDatabase(null, db, false);
    }
}
