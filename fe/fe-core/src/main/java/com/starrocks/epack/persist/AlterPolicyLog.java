// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.epack.sql.ast.PolicyName;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AlterPolicyLog implements Writable {
    @SerializedName(value = "pn")
    private final PolicyName policyName;

    @SerializedName(value = "pt")
    private final PolicyType policyType;

    @SerializedName(value = "plog")
    private final AlterPolicyClauseInfo alterPolicyClauseInfo;

    public AlterPolicyLog(PolicyName policyName, PolicyType policyType, AlterPolicyClauseInfo alterPolicyClauseInfo) {
        this.policyName = policyName;
        this.policyType = policyType;
        this.alterPolicyClauseInfo = alterPolicyClauseInfo;
    }

    public PolicyName getPolicyName() {
        return policyName;
    }

    public PolicyType getPolicyType() {
        return policyType;
    }

    public AlterPolicyClauseInfo getAlterPolicyClauseInfo() {
        return alterPolicyClauseInfo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static AlterPolicyLog read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, AlterPolicyLog.class);
    }

    public abstract static class AlterPolicyClauseInfo {
    }

    public static class PolicyRenameInfo extends AlterPolicyClauseInfo {
        @SerializedName(value = "nn")
        private final String newPolicyName;

        public PolicyRenameInfo(String newPolicyName) {
            this.newPolicyName = newPolicyName;
        }

        public String getNewPolicyName() {
            return newPolicyName;
        }
    }

    public static class PolicySetBodyInfo extends AlterPolicyClauseInfo {
        @SerializedName(value = "b")
        private final String policyBody;

        public PolicySetBodyInfo(String policyBody) {
            this.policyBody = policyBody;
        }

        public String getPolicyBody() {
            return policyBody;
        }
    }

    public static class PolicySetCommentInfo extends AlterPolicyClauseInfo {
        @SerializedName(value = "c")
        private final String comment;

        public PolicySetCommentInfo(String comment) {
            this.comment = comment;
        }

        public String getComment() {
            return comment;
        }
    }
}
