// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.privilege.DbUID;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DropPolicyLog implements Writable {
    @SerializedName(value = "t")
    private PolicyType policyType;

    @SerializedName(value = "i")
    private final Long policyId;

    @SerializedName(value = "d")
    private DbUID db;

    @SerializedName(value = "n")
    private String name;

    public DropPolicyLog(PolicyType policyType, Long policyId, DbUID db, String name) {
        this.policyType = policyType;
        this.policyId = policyId;

        this.db = db;
        this.name = name;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public DbUID getDb() {
        return db;
    }

    public String getName() {
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static DropPolicyLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, DropPolicyLog.class);
    }
}
