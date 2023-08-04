// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.starrocks.common.io.Text;
import com.starrocks.persist.GenericNameWithPropsPersistInfo;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

public class RoleMappingPersistInfo extends GenericNameWithPropsPersistInfo {
    public RoleMappingPersistInfo(String name, Map<String, String> propertyMap) {
        super(name, propertyMap);
    }

    public static RoleMappingPersistInfo read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RoleMappingPersistInfo.class);
    }
}
