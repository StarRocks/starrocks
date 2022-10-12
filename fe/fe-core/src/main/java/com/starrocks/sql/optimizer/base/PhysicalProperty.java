// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.sql.optimizer.base;

import com.starrocks.sql.optimizer.Group;
import com.starrocks.sql.optimizer.GroupExpression;

// The required physical property
public interface PhysicalProperty extends Property {
    boolean isSatisfy(PhysicalProperty other);

    // append enforcers to the child
    GroupExpression appendEnforcers(Group child);
}
