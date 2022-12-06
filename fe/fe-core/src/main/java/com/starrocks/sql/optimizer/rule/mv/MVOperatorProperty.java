// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.mv;

public class MVOperatorProperty {
    private KeyInference.KeyPropertySet keySet;
    private ModifyInference.ModifyOp modify;

    public MVOperatorProperty() {
    }

    public MVOperatorProperty(KeyInference.KeyPropertySet keySet, ModifyInference.ModifyOp modify) {
        this.keySet = keySet;
        this.modify = modify;
    }

    public KeyInference.KeyPropertySet getKeySet() {
        return keySet;
    }

    public ModifyInference.ModifyOp getModify() {
        return modify;
    }

    public void setKeySet(KeyInference.KeyPropertySet keySet) {
        this.keySet = keySet;
    }

    public void setModify(ModifyInference.ModifyOp modify) {
        this.modify = modify;
    }
}
