// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.physical.stream;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.ModifyInference;

public class PhysicalStreamOperator extends PhysicalOperator {

    private KeyInference.KeyPropertySet keyPropertySet;
    private ModifyInference.ModifyOp modifyOp;

    protected PhysicalStreamOperator(OperatorType type) {
        super(type);
    }

    public void addKeyProperty(KeyInference.KeyProperty keyProperty) {
        if (keyPropertySet == null) {
            this.keyPropertySet = new KeyInference.KeyPropertySet();
        }
        this.keyPropertySet.addKey(keyProperty);
    }

    public void setKeyPropertySet(KeyInference.KeyPropertySet keyPropertySet) {
        Preconditions.checkState(this.keyPropertySet == null);
        this.keyPropertySet = keyPropertySet;
    }

    public KeyInference.KeyPropertySet getKeyPropertySet() {
        return keyPropertySet;
    }

    public void setModifyOp(ModifyInference.ModifyOp modifyOp) {
        this.modifyOp = modifyOp;
    }

    public ModifyInference.ModifyOp getModifyOp() {
        return this.modifyOp;
    }

}
