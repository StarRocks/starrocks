// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.scheduler.mv;

/**
 * EpochCoordinator coordinate epoch progress of executors
 */
public interface EpochCoordinator {

    void runEpoch(MVEpoch epoch);

}
