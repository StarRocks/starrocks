// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.mysql.privilege;

public enum AuthPlugin {
    MYSQL_NATIVE_PASSWORD,
    AUTHENTICATION_LDAP_SIMPLE,
    AUTHENTICATION_KERBEROS
}
