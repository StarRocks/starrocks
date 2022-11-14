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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.starrocks.mysql.privilege.PrivBitSet;
import com.starrocks.mysql.privilege.Privilege;

import java.util.List;

public enum AccessPrivilege {
    READ_ONLY(1, "READ_ONLY"),
    READ_WRITE(2, "READ_WRITE"),
    ALL(3, "ALL"),
    NODE_PRIV(4, "Privilege for cluster node operations"),
    GRANT_PRIV(5, "Privilege for granting privlege"),
    SELECT_PRIV(6, "Privilege for select data in tables"),
    LOAD_PRIV(7, "Privilege for loading data into tables"),
    ALTER_PRIV(8, "Privilege for alter database or table"),
    CREATE_PRIV(9, "Privilege for createing database or table"),
    DROP_PRIV(10, "Privilege for dropping database or table"),
    ADMIN_PRIV(11, "All privileges except NODE_PRIV"),
    USAGE_PRIV(12, "Privileage for use resource"),
    IMPERSONATE_PRIV(13, "Privilege for impersonate as other user");

    private int flag;
    private String desc;

    private AccessPrivilege(int flag, String desc) {
        this.flag = flag;
        this.desc = desc;
    }

    public PrivBitSet toPrivilege() {
        Preconditions.checkState(flag > 0 && flag < 14);
        switch (flag) {
            case 1:
                return PrivBitSet.of(Privilege.SELECT_PRIV);
            case 2:
            case 3:
                return PrivBitSet.of(Privilege.SELECT_PRIV, Privilege.LOAD_PRIV,
                        Privilege.ALTER_PRIV, Privilege.CREATE_PRIV,
                        Privilege.DROP_PRIV);
            case 4:
                return PrivBitSet.of(Privilege.NODE_PRIV);
            case 5:
                return PrivBitSet.of(Privilege.GRANT_PRIV);
            case 6:
                return PrivBitSet.of(Privilege.SELECT_PRIV);
            case 7:
                return PrivBitSet.of(Privilege.LOAD_PRIV);
            case 8:
                return PrivBitSet.of(Privilege.ALTER_PRIV);
            case 9:
                return PrivBitSet.of(Privilege.CREATE_PRIV);
            case 10:
                return PrivBitSet.of(Privilege.DROP_PRIV);
            case 11:
                return PrivBitSet.of(Privilege.ADMIN_PRIV);
            case 12:
                return PrivBitSet.of(Privilege.USAGE_PRIV);
            case 13:
                return PrivBitSet.of(Privilege.IMPERSONATE_PRIV);
            default:
                return null;
        }
    }

    public static AccessPrivilege fromName(String privStr) {
        try {
            return AccessPrivilege.valueOf(privStr.toUpperCase());
        } catch (Exception e) {
            return null;
        }
    }

    public static AccessPrivilege merge(List<AccessPrivilege> privileges) {
        if (privileges == null || privileges.isEmpty()) {
            return null;
        }

        AccessPrivilege privilege = null;
        for (AccessPrivilege iter : privileges) {
            if (privilege == null) {
                privilege = iter;
            } else {
                if (iter.flag > privilege.flag) {
                    privilege = iter;
                }
            }
        }

        return privilege;
    }

    @Override
    public String toString() {
        return desc;
    }
}
