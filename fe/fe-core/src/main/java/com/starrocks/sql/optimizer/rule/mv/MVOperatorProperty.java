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
