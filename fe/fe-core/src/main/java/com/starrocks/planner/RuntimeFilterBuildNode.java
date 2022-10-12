// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.planner;

import com.starrocks.common.IdGenerator;

import java.util.List;

public interface RuntimeFilterBuildNode {
    List<RuntimeFilterDescription> getBuildRuntimeFilters();

    void buildRuntimeFilters(IdGenerator<RuntimeFilterId> runtimeFilterIdIdGenerator);

    void clearBuildRuntimeFilters();
}
