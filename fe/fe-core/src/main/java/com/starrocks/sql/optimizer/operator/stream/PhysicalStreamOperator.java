// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.operator.stream;

import com.google.common.base.Preconditions;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.physical.PhysicalOperator;
import com.starrocks.sql.optimizer.rule.mv.KeyInference;
import com.starrocks.sql.optimizer.rule.mv.MVOperatorProperty;
import com.starrocks.sql.optimizer.rule.mv.ModifyInference;

public class PhysicalStreamOperator extends PhysicalOperator {

    private final MVOperatorProperty property = new MVOperatorProperty();

    protected PhysicalStreamOperator(OperatorType type) {
        super(type);
    }

    public void setKeyPropertySet(KeyInference.KeyPropertySet keyPropertySet) {
        Preconditions.checkState(this.property.getKeySet() == null);
        this.property.setKeySet(keyPropertySet);
    }

    public KeyInference.KeyPropertySet getKeyPropertySet() {
        return property.getKeySet();
    }

    public void setModifyOp(ModifyInference.ModifyOp modifyOp) {
        this.property.setModify(modifyOp);
    }

    public ModifyInference.ModifyOp getModifyOp() {
        return this.property.getModify();
    }

}
