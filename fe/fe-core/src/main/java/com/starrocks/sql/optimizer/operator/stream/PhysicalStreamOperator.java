// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
