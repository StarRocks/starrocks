// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.
package com.starrocks.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.MaterializedView;
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

    public RenameMaterializedViewLog(MaterializedView materializedView, String oldMaterializedViewName,
                                     String newMaterializedViewName) {

        this.id = materializedView.getId();
        this.dbId = materializedView.getDbId();
        this.oldMaterializedViewName = oldMaterializedViewName;
        this.newMaterializedViewName = newMaterializedViewName;
    }

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public String getOldMaterializedViewName() {
        return oldMaterializedViewName;
    }

    public String getNewMaterializedViewName() {
        return newMaterializedViewName;
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

}
