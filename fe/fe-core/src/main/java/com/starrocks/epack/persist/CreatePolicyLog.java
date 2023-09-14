// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Type;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.privilege.DbUID;
import com.starrocks.epack.privilege.Policy;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

public class CreatePolicyLog implements Writable {

    private static final Logger LOG = LogManager.getLogger(CreatePolicyLog.class);

    @SerializedName(value = "type")
    private PolicyType policyType;

    @SerializedName(value = "id")
    private final Long policyId;

    @SerializedName(value = "name")
    private String name;

    @SerializedName(value = "db")
    private DbUID dbUID;

    @SerializedName(value = "argNames")
    private List<String> argNames;

    @SerializedName(value = "argTypes")
    private List<Type> argTypes;

    @SerializedName(value = "retType")
    private Type retType;

    @SerializedName(value = "p")
    private String policyExpressionSQL;

    @SerializedName(value = "sqlMode")
    private long sqlMode = 0L;

    @SerializedName(value = "comment")
    private String comment;

    public CreatePolicyLog(Policy policy) {
        this.policyType = policy.getPolicyType();
        this.policyId = policy.getPolicyId();
        this.name = policy.getName();
        this.dbUID = policy.getDbUID();
        this.argNames = policy.getArgNames();
        this.argTypes = policy.getArgTypes();
        this.retType = policy.getRetType();
        this.policyExpressionSQL = policy.getPolicyExpressionSQL();
        this.comment = policy.getComment();
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public Long getPolicyId() {
        return policyId;
    }

    public String getName() {
        return name;
    }

    public DbUID getDbUID() {
        return dbUID;
    }

    public List<String> getArgNames() {
        return argNames;
    }

    public List<Type> getArgTypes() {
        return argTypes;
    }

    public Type getRetType() {
        return retType;
    }

    public String getPolicyExpressionSQL() {
        return policyExpressionSQL;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static CreatePolicyLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, CreatePolicyLog.class);
    }
}
