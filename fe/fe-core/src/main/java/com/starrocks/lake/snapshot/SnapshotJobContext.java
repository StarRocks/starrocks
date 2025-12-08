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

package com.starrocks.lake.snapshot;

import com.starrocks.common.Pair;
import com.starrocks.leader.CheckpointController;

/**
 * Context interface for snapshot job execution.
 * Provides access to external dependencies required by snapshot jobs,
 * allowing different job types to use different implementations.
 */
public interface SnapshotJobContext {
    /**
     * Get FE checkpoint controller
     */
    CheckpointController getFeController();

    /**
     * Get StarMgr checkpoint controller
     */
    CheckpointController getStarMgrController();

    /**
     * Capture consistent checkpoint IDs between FE and StarMgr.
     * This method coordinates the checkpoint process to ensure consistency.
     * 
     * @return Pair of (FE journal ID, StarMgr journal ID) if successful, null
     *         otherwise
     */
    Pair<Long, Long> captureConsistentCheckpointIdBetweenFEAndStarMgr();
}