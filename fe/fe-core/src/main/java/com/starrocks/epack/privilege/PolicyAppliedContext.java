// Copyright 2021-present StarRocks, Inc. All rights reserved.

package com.starrocks.epack.privilege;

import com.google.gson.annotations.SerializedName;
import com.starrocks.epack.sql.ast.PolicyType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PolicyAppliedContext {
    @SerializedName(value = "m")
    private final Map<String, MaskingPolicyContext> maskingPolicyApply;

    @SerializedName(value = "r")
    private final List<RowAccessPolicyContext> rowAccessPolicyApply;

    private final ReentrantReadWriteLock contextLock;

    public PolicyAppliedContext() {
        rowAccessPolicyApply = new ArrayList<>();
        maskingPolicyApply = new HashMap<>();
        contextLock = new ReentrantReadWriteLock();
    }

    public void applyMaskingPolicy(String maskingColumn, MaskingPolicyContext columnMaskingPolicyContext) {
        contextLock.writeLock().lock();
        try {
            maskingPolicyApply.put(maskingColumn, columnMaskingPolicyContext);
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public void revokeMaskingPolicy(String maskingColumn) {
        contextLock.writeLock().lock();
        try {
            maskingPolicyApply.remove(maskingColumn);
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public Map<String, MaskingPolicyContext> getMaskingPolicyApply() {
        contextLock.readLock().lock();
        try {
            return maskingPolicyApply;
        } finally {
            contextLock.readLock().unlock();
        }
    }

    public void addRowAccessPolicy(RowAccessPolicyContext withRowAccessPolicy) {
        contextLock.writeLock().lock();
        try {
            rowAccessPolicyApply.add(withRowAccessPolicy);
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public void revokeMaskingPolicy(Long policyId) {
        contextLock.writeLock().lock();

        try {
            Iterator<Map.Entry<String, MaskingPolicyContext>> maskingPolicyContextIterator
                    = maskingPolicyApply.entrySet().iterator();
            while (maskingPolicyContextIterator.hasNext()) {
                Map.Entry<String, MaskingPolicyContext> entry = maskingPolicyContextIterator.next();
                MaskingPolicyContext withColumnMaskingPolicy = entry.getValue();
                if (withColumnMaskingPolicy.getPolicyId().equals(policyId)) {
                    maskingPolicyContextIterator.remove();
                }
            }
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public void revokeRowAccessPolicy(Long policyId) {
        contextLock.writeLock().lock();

        try {
            rowAccessPolicyApply.removeIf(withRowAccessPolicy -> withRowAccessPolicy.getPolicyId().equals(policyId));
        } finally {
            contextLock.writeLock().unlock();
        }
    }

    public void clearRowAccessPolicy() {
        rowAccessPolicyApply.clear();
    }

    public List<RowAccessPolicyContext> getRowAccessPolicyApply() {
        contextLock.readLock().lock();
        try {
            return rowAccessPolicyApply;
        } finally {
            contextLock.readLock().unlock();
        }
    }

    public boolean hasApplyPolicy(PolicyType policyType, Long policyId) {
        contextLock.readLock().lock();
        try {
            if (policyType.equals(PolicyType.MASKING)) {
                return maskingPolicyApply.values().stream().anyMatch(
                        columnMaskingPolicyContext -> columnMaskingPolicyContext.policyId.equals(policyId));
            } else {
                return rowAccessPolicyApply.stream().anyMatch(
                        rowAccessPolicyContext -> rowAccessPolicyContext.policyId.equals(policyId));
            }
        } finally {
            contextLock.readLock().unlock();
        }
    }

    public boolean isEmpty() {
        return maskingPolicyApply.isEmpty() && rowAccessPolicyApply.isEmpty();
    }
}