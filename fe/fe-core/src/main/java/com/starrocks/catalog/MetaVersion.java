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


package com.starrocks.catalog;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksFEMetaVersion;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MetaVersion implements Writable {
    private static final String KEY_COMMUNITY_VERSION = "communityVersion";

    // Before version 1.19, the json key for storing starrocksVersion is KEY_DORISDB_VERSION,
    // and the later versions are KEY_STARROCKS_VERSION
    private static final String KEY_STARROCKS_VERSION = "starrocksVersion";
    private static final String KEY_DORISDB_VERSION = "dorisDBVersion";

    private int starrocksVersion;

    public MetaVersion(int starrocksVersion) {
        this.starrocksVersion = starrocksVersion;
    }

    public int getStarRocksVersion() {
        return starrocksVersion;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JsonObject jsonObject = new JsonObject();
        // For rollback compatibility
        jsonObject.addProperty(KEY_COMMUNITY_VERSION, FeConstants.META_VERSION);

        // For rollback compatibility, save the starrocksVersion both to
        // KEY_STARROCKS_VERSION and KEY_DORISDB_VERSION
        jsonObject.addProperty(KEY_STARROCKS_VERSION, starrocksVersion);
        jsonObject.addProperty(KEY_DORISDB_VERSION, starrocksVersion);
        Text.writeString(out, jsonObject.toString());
    }

    public static MetaVersion read(DataInput in) throws IOException {
        String json = Text.readString(in);
        JsonObject jsonObject = JsonParser.parseString(json).getAsJsonObject();
        int starrocksVersion;
        if (jsonObject.has(KEY_STARROCKS_VERSION)) {
            starrocksVersion = jsonObject.getAsJsonPrimitive(KEY_STARROCKS_VERSION).getAsInt();
        } else {
            // For compatibility, the json key before 1.19 version is KEY_DORISDB_VERSION
            starrocksVersion = jsonObject.getAsJsonPrimitive(KEY_DORISDB_VERSION).getAsInt();
        }
        return new MetaVersion(starrocksVersion);
    }

    public static boolean isCompatible(long dataVersion, long codeVersion) {
        if (dataVersion <= codeVersion) {
            return true;
        }

        if (dataVersion == StarRocksFEMetaVersion.VERSION_4 && codeVersion == StarRocksFEMetaVersion.VERSION_3) {
            return true;
        }
        return false;
    }
}
