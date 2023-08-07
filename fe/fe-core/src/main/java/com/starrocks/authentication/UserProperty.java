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


package com.starrocks.authentication;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.SetUserPropertyVar;

import java.util.List;

public class UserProperty {
    @SerializedName(value = "m")
    private long maxConn = 100;

    private static final String PROP_MAX_USER_CONNECTIONS = "max_user_connections";

    public long getMaxConn() {
        return maxConn;
    }

    public void setMaxConn(long maxConn) {
        this.maxConn = maxConn;
    }

    public void update(List<Pair<String, String>> properties) throws DdlException {
        // copy
        long newMaxConn = maxConn;

        // update
        for (Pair<String, String> entry : properties) {
            String key = entry.first;
            String value = entry.second;

            String[] keyArr = key.split("\\" + SetUserPropertyVar.DOT_SEPARATOR);
            if (keyArr[0].equalsIgnoreCase(PROP_MAX_USER_CONNECTIONS)) {
                if (keyArr.length != 1) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " format error");
                }

                try {
                    newMaxConn = Long.parseLong(value);
                } catch (NumberFormatException e) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS + " is not a number");
                }

                if (newMaxConn <= 0 || newMaxConn > 10000) {
                    throw new DdlException(PROP_MAX_USER_CONNECTIONS +
                            " is not valid, the value must be between 1 and 10000");
                }

                if (newMaxConn > Config.qe_max_connection) {
                    throw new DdlException(
                            PROP_MAX_USER_CONNECTIONS +
                                    " is not valid, the value must be less than qe_max_connection("
                                    + Config.qe_max_connection + ")");
                }
            } else {
                throw new DdlException("Unknown user property(" + key + ")");
            }
        }

        // set
        maxConn = newMaxConn;
    }
}
