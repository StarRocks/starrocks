// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/FeNameFormat.java

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

package com.starrocks.common;

import com.google.common.base.Strings;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.mysql.privilege.Role;
import com.starrocks.sql.analyzer.SemanticException;

public class FeNameFormat {
    private FeNameFormat() {}

    private static final String LABEL_REGEX = "^[-\\w]{1,128}$";
    public static final String COMMON_NAME_REGEX = "^[a-zA-Z]\\w{0,63}$|^_[a-zA-Z0-9]\\w{0,62}$";

    // The length length of db name is 256
    public static final String DB_NAME_REGEX = "^[a-zA-Z]\\w{0,255}$|^_[a-zA-Z0-9]\\w{0,254}$";

    public static final String TABLE_NAME_REGEX = "^[^\0]{1,1024}$";
    // Now we can not accept all characters because current design of delete save delete cond contains column name,
    // so it can not distinguish whether it is an operator or a column name
    // the future new design will improve this problem and open this limitation
    private static final String COLUMN_NAME_REGEX = "^[^\0=<>!\\*]{1,1024}$";

    // The username  by kerberos authentication may include the host name, so additional adaptation is required.
    private static final String MYSQL_USER_NAME_REGEX = "^\\w{1,64}/?[.\\w-]{0,63}$";

    public static final String FORBIDDEN_PARTITION_NAME = "placeholder_";

    public static void checkDbName(String dbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(dbName) || !dbName.matches(DB_NAME_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }
    }

    public static void checkTableName(String tableName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName) || !tableName.matches(TABLE_NAME_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_TABLE_NAME, tableName);
        }
    }

    public static void checkPartitionName(String partitionName) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName) || !partitionName.matches(COMMON_NAME_REGEX)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }

        if (partitionName.startsWith(FORBIDDEN_PARTITION_NAME)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }
    }

    public static void checkColumnName(String columnName) {
        if (Strings.isNullOrEmpty(columnName) || !columnName.matches(COLUMN_NAME_REGEX)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        }
        if (columnName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        }
        if (columnName.startsWith(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
        }
    }

    public static void checkLabel(String label) {
        if (Strings.isNullOrEmpty(label) || !label.matches(LABEL_REGEX)) {
            throw new SemanticException("Label format error. regex: " + LABEL_REGEX + ", label: " + label);
        }
    }

    public static void checkUserName(String userName) throws AnalysisException {
        if (Strings.isNullOrEmpty(userName) || !userName.matches(MYSQL_USER_NAME_REGEX) || userName.length() > 64) {
            throw new AnalysisException("invalid user name: " + userName);
        }
    }

    public static void checkRoleName(String role, boolean canBeAdmin, String errMsg) throws AnalysisException {
        if (Strings.isNullOrEmpty(role) || !role.matches(COMMON_NAME_REGEX)) {
            throw new AnalysisException("invalid role format: " + role);
        }

        boolean res = false;
        if (CaseSensibility.ROLE.getCaseSensibility()) {
            res = role.equals(Role.OPERATOR_ROLE) || (!canBeAdmin && role.equals(Role.ADMIN_ROLE));
        } else {
            res = role.equalsIgnoreCase(Role.OPERATOR_ROLE)
                    || (!canBeAdmin && role.equalsIgnoreCase(Role.ADMIN_ROLE));
        }

        if (res) {
            throw new AnalysisException(errMsg + ": " + role);
        }
    }

    public static void checkResourceName(String resourceName) throws AnalysisException {
        checkCommonName("resource", resourceName);
    }

    public static void checkCatalogName(String catalogName) throws AnalysisException {
        checkCommonName("catalog", catalogName);
    }

    public static void checkCommonName(String type, String name) {
        if (Strings.isNullOrEmpty(name) || !name.matches(COMMON_NAME_REGEX)) {
            ErrorReport.reportSemanticException(ErrorCode.ERR_WRONG_NAME_FORMAT, type, name);
        }
    }
}
