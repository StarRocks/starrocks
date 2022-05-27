// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RenameMaterializedViewLog implements Writable {

    @SerializedName(value = "MaterializedViewId")
    private long id;

    @SerializedName(value = "dbId")
    private long dbId;

    @SerializedName(value = "oldMaterializedViewName")
    private String oldMaterializedViewName;

    @SerializedName(value = "newMaterializedViewName")
    private String newMaterializedViewName;

    public RenameMaterializedViewLog() {
    }

    public RenameMaterializedViewLog(long id, long dbId, String oldMaterializedViewName,
                                     String newMaterializedViewName) {
        this.id = id;
        this.dbId = dbId;
        this.oldMaterializedViewName = oldMaterializedViewName;
        this.newMaterializedViewName = newMaterializedViewName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static RenameMaterializedViewLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, RenameMaterializedViewLog.class);
    }

    public void readFields(DataInput in) throws IOException {
        dbId = in.readLong();
        id = in.readLong();
        oldMaterializedViewName = Text.readString(in);
        newMaterializedViewName = Text.readString(in);
    }
}
