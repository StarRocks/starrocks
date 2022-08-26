// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.alter;

import com.starrocks.common.NotImplementedException;
import com.starrocks.common.UserException;

public class LakeTableAlterJobV2Builder extends AlterJobV2Builder {
    @Override
    public AlterJobV2 build() throws UserException {
        throw new NotImplementedException("does not support alter lake table yet");
    }
}
