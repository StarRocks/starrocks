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

package com.starrocks.mysql.privilege;

public enum Privilege {
    NODE_PRIV("Node_priv", 0, "Privilege for cluster node operations"),
    ADMIN_PRIV("Admin_priv", 1, "Privilege for admin user"),
    GRANT_PRIV("Grant_priv", 2, "Privilege for granting privilege"),
    SELECT_PRIV("Select_priv", 3, "Privilege for select data in tables"),
    LOAD_PRIV("Load_priv", 4, "Privilege for loading data into tables"),
    ALTER_PRIV("Alter_priv", 5, "Privilege for alter database or table"),
    CREATE_PRIV("Create_priv", 6, "Privilege for createing database or table"),
    DROP_PRIV("Drop_priv", 7, "Privilege for dropping database or table"),
    USAGE_PRIV("Usage_priv", 8, "Privilege for using resource"),
    IMPERSONATE_PRIV("Impersonate_priv", 9, "Privilege for impersonate as other user");

    public static Privilege[] privileges = {
            NODE_PRIV,
            ADMIN_PRIV,
            GRANT_PRIV,
            SELECT_PRIV,
            LOAD_PRIV,
            ALTER_PRIV,
            CREATE_PRIV,
            DROP_PRIV,
            USAGE_PRIV,
            IMPERSONATE_PRIV
    };

    private String name;
    private int idx;
    private String desc;

    private Privilege(String name, int index, String desc) {
        this.name = name;
        this.idx = index;
        this.desc = desc;
    }

    public String getName() {
        return name;
    }

    public String getUpperNameForMysql() {
        return name.toUpperCase().replace("_PRIV", "");
    }

    public int getIdx() {
        return idx;
    }

    public String getDesc() {
        return desc;
    }

    public static Privilege getPriv(int index) {
        if (index < 0 || index > Privilege.values().length - 1) {
            return null;
        }
        return privileges[index];
    }

    public static boolean satisfy(PrivBitSet grantPriv, PrivPredicate wanted) {
        return grantPriv.satisfy(wanted);
    }

    @Override
    public String toString() {
        return name;
    }
}
