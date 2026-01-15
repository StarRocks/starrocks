// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.persist.gson;

import com.google.gson.annotations.SerializedName;
import com.starrocks.epack.persist.AlterPolicyLog;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.sql.ast.PolicyName;
import com.starrocks.sql.parser.NodePosition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;

class GsonSerializationTest {
    private static class ArrayBlockingQueueAdapterTest {
        @SerializedName(value = "queue")
        private final ArrayBlockingQueue<String> queue = new ArrayBlockingQueue<>(10);

        public void init() {
            int size = queue.remainingCapacity() / 2;
            for (int i = 0; i < size; ++i) {
                queue.add(String.valueOf(i));
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            ArrayBlockingQueueAdapterTest other = (ArrayBlockingQueueAdapterTest) obj;
            return Objects.equals(queue, other.queue) && queue.remainingCapacity() == other.queue.remainingCapacity();
        }
    }

    @Test
    public void testArrayBlockingQueueAdapter() {
        ArrayBlockingQueueAdapterTest adapterTest = new ArrayBlockingQueueAdapterTest();
        adapterTest.init();

        ArrayBlockingQueueAdapterTest adapterTest2 = GsonUtils.GSON.fromJson(
                GsonUtils.GSON.toJson(adapterTest), ArrayBlockingQueueAdapterTest.class);

        Assert.assertEquals(adapterTest, adapterTest2);
    }

    @Test
    public void testAlterPolicyLogSerialization() {
        // Test PolicyRenameInfo
        PolicyName policyName = new PolicyName("catalog", "db", "policy1", NodePosition.ZERO);
        AlterPolicyLog.PolicyRenameInfo renameInfo = new AlterPolicyLog.PolicyRenameInfo("newPolicyName");
        AlterPolicyLog renameLog = new AlterPolicyLog(policyName, PolicyType.MASKING, renameInfo);

        String renameJson = GsonUtils.GSON.toJson(renameLog);
        AlterPolicyLog deserializedRenameLog = GsonUtils.GSON.fromJson(renameJson, AlterPolicyLog.class);

        Assert.assertNotNull(deserializedRenameLog);
        Assert.assertEquals("newPolicyName", 
                ((AlterPolicyLog.PolicyRenameInfo) deserializedRenameLog.getAlterPolicyClauseInfo()).getNewPolicyName());

        // Test PolicySetBodyInfo
        AlterPolicyLog.PolicySetBodyInfo bodyInfo = new AlterPolicyLog.PolicySetBodyInfo("SELECT * FROM table");
        AlterPolicyLog bodyLog = new AlterPolicyLog(policyName, PolicyType.ROW_ACCESS, bodyInfo);

        String bodyJson = GsonUtils.GSON.toJson(bodyLog);
        AlterPolicyLog deserializedBodyLog = GsonUtils.GSON.fromJson(bodyJson, AlterPolicyLog.class);

        Assert.assertNotNull(deserializedBodyLog);
        Assert.assertEquals("SELECT * FROM table", 
                ((AlterPolicyLog.PolicySetBodyInfo) deserializedBodyLog.getAlterPolicyClauseInfo()).getPolicyBody());

        // Test PolicySetCommentInfo
        AlterPolicyLog.PolicySetCommentInfo commentInfo = new AlterPolicyLog.PolicySetCommentInfo("test comment");
        AlterPolicyLog commentLog = new AlterPolicyLog(policyName, PolicyType.MASKING, commentInfo);

        String commentJson = GsonUtils.GSON.toJson(commentLog);
        AlterPolicyLog deserializedCommentLog = GsonUtils.GSON.fromJson(commentJson, AlterPolicyLog.class);

        Assert.assertNotNull(deserializedCommentLog);
        Assert.assertEquals("test comment", 
                ((AlterPolicyLog.PolicySetCommentInfo) deserializedCommentLog.getAlterPolicyClauseInfo()).getComment());
    }
}