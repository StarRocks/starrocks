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

package com.starrocks.sql.optimizer.operator.scalar;

import com.google.common.collect.Lists;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.type.Type;

import java.util.List;
import java.util.Objects;

public abstract class ArgsScalarOperator extends ScalarOperator {
    protected List<ScalarOperator> arguments = Lists.newArrayList();

    public ArgsScalarOperator(OperatorType opType, Type type) {
        super(opType, type);
    }

    @Override
    public List<ScalarOperator> getChildren() {
        return arguments;
    }

    @Override
    public ScalarOperator getChild(int index) {
        return arguments.get(index);
    }

    @Override
    public void setChild(int index, ScalarOperator child) {
        arguments.set(index, child);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hashCodeSelf(), arguments);
    }

    @Override
    public boolean equals(Object obj) {
        return equalsSelf(obj) && Objects.equals(arguments, ((ArgsScalarOperator) obj).arguments);
    }

    @Override
    public boolean equalsSelf(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ArgsScalarOperator other = (ArgsScalarOperator) obj;
        return Objects.equals(this.opType, other.opType) &&
               Objects.equals(this.type, other.type);
    }
}
