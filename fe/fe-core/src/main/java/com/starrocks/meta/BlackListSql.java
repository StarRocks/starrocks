// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.meta;

import java.util.regex.Pattern;

public class BlackListSql {
    public BlackListSql(Pattern pattern, long id) {
        this.pattern = pattern;
        this.id = id;
    }

    public Pattern pattern;
    public long id;
}
