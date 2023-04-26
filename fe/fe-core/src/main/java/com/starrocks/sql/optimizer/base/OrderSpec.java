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


package com.starrocks.sql.optimizer.base;

import com.google.common.collect.Lists;

import java.util.List;
import java.util.Objects;

public class OrderSpec {
    private final List<Ordering> orderDescs;

    public OrderSpec(List<Ordering> orderDescs) {
        this.orderDescs = orderDescs;
    }

    public List<Ordering> getOrderDescs() {
        return orderDescs;
    }

    public boolean isSatisfy(OrderSpec rhs) {
        if (orderDescs.size() < rhs.getOrderDescs().size()) {
            return false;
        }

        for (int i = 0; i < rhs.getOrderDescs().size(); ++i) {
            if (!orderDescs.get(i).matches(rhs.getOrderDescs().get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderDescs);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof OrderSpec)) {
            return false;
        }

        OrderSpec rhs = (OrderSpec) obj;
        if (orderDescs.size() != rhs.orderDescs.size()) {
            return false;
        }
        for (int i = 0; i < orderDescs.size(); ++i) {
            if (!orderDescs.get(i).equals(rhs.orderDescs.get(i))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return orderDescs.toString();
    }

    public static OrderSpec createEmpty() {
        return new OrderSpec(Lists.newArrayList());
    }
}
