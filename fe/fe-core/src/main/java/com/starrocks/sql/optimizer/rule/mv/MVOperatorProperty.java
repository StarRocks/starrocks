// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.rule.mv;

public class MVOperatorProperty {
    public final KeyInference.KeyPropertySet keySet;
    public final ModifyInference.ModifyOp modify;

    public MVOperatorProperty(KeyInference.KeyPropertySet keySet, ModifyInference.ModifyOp modify) {
        this.keySet = keySet;
        this.modify = modify;
    }

}
