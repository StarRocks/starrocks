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

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

public class Catalog implements Writable {
    public static final String CATALOG_TYPE = "type";

    // old external table uuid format: external_catalog_name.db_name.table_name.creation_time
    // new external table uuid format: table_name
    // internal table uuid format: table_id
    public static String getCompatibleTableUUID(String uuid) {
        return uuid.contains(".") ? uuid.split("\\.")[2] : uuid;
    }

    // old database uuid format: external_catalog_name.db_name
    // new database uuid format: db_name
    // internal database uuid format: db_id
    public static String getCompatibleDbUUID(String uuid) {
        return uuid.contains(".") ? uuid.split("\\.")[1] : uuid;
    }

    // Reserved fields for later support operations such as rename
    @SerializedName("id")
    protected long id;
    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "comment")
    protected String comment;
    @SerializedName(value = "config")
    protected Map<String, String> config;

    public Catalog(long id, String name, Map<String, String> config, String comment) {
        this.id = id;
        this.name = name;
        this.config = config;
        this.comment = comment;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return config.get(CATALOG_TYPE);
    }

    public Map<String, String> getConfig() {
        return config;
    }

    public String getComment() {
        return comment;
    }

    public String getDisplayComment() {
        return CatalogUtils.addEscapeCharacter(comment);
    }

    public void getProcNodeData(BaseProcResult result) {
        result.addRow(Lists.newArrayList(this.getName(), StringUtils.capitalize(config.get(CATALOG_TYPE)), this.getComment()));
    }

    public static Catalog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Catalog.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }
}
