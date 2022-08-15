// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.common;

public class StarRocksFEMetaVersion {
    //no use version
    public static final int VERSION_1 = 1;

    //add user identification info
    public static final int VERSION_2 = 2;

    //support hive external read
    public static final int VERSION_3 = 3;

    // note: when increment meta version, should assign the latest version to VERSION_CURRENT
    public static final int VERSION_CURRENT = VERSION_3;
}
