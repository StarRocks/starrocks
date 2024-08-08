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

package com.starrocks.sql.automv.lifecycle;

import com.google.common.base.Preconditions;

public class TransferGuards {

    public static final TransferGuard CRADLE_TO_INTERN_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_CRADLE_TO_INTERN);
        return mvLifecycle.isActive() && mvLifecycle.hasRefreshed();
    });
    public static final TransferGuard GRADLE_TO_GRAVE_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_CRADLE_TO_GRAVE);
        return mvLifecycle.isInactive();
    });
    public static final TransferGuard CRADLE_TO_EXTINCTION_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_CRADLE_TO_EXTINCTION);
        if (mvLifecycle.isDetached() && policy.getInfantAbortionDictator().test(mvLifecycle)) {
            return true;
        }

        if (mvLifecycle.isDetached()) {
            return false;
        }

        if (mvLifecycle.isAbsent()) {
            return true;
        }

        return mvLifecycle.neverRefreshed() && policy.getInitialRefreshFailureDictator().test(mvLifecycle);
    });
    public static final TransferGuard INTERN_TO_TENURED_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_INTERN_TO_TENURED);
        if (mvLifecycle.isActive() && mvLifecycle.passInternship(policy)) {
            return policy.getGoodMVDictator().apply(mvLifecycle, policy.getMVHitRatioProvider());
        }
        return false;
    });
    public static final TransferGuard INTERN_TO_RETIRED_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_INTERN_TO_RETIRED);
        if (mvLifecycle.isActive() && mvLifecycle.passInternship(policy)) {
            return policy.getBadMVDictator().apply(mvLifecycle, policy.getMVHitRatioProvider());
        }
        return false;
    });
    public static final TransferGuard INTERN_TO_GRAVE_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_INTERN_TO_GRAVE);
        return mvLifecycle.isAbsent() || mvLifecycle.isInactive();
    });
    public static final TransferGuard TENURED_TO_RETIRED_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_TENURED_TO_RETIRED);
        return mvLifecycle.isActive() && MVLifecycle.whenMVPresentThen(mvLifecycle,
                (lifecycle, db, mv) -> policy.getUnsatisfactoryPerformanceDictator()
                        .apply(mvLifecycle, policy.getMVHitRatioProvider()));
    });
    public static final TransferGuard TENURED_TO_GRAVE_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_TENURED_TO_GRAVE);
        return mvLifecycle.isAbsent() || mvLifecycle.isInactive();
    });
    public static final TransferGuard RETIRED_TO_INTERN_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_RETIRED_TO_INTERN);
        return mvLifecycle.isActive() && MVLifecycle.whenMVPresentThen(mvLifecycle,
                (lifecycle, db, mv) -> policy.getExcellentPerformanceDictator()
                        .apply(mvLifecycle, policy.getMVHitRatioProvider()));
    });

    public static final TransferGuard RETIRED_TO_GRAVE_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_RETIRED_TO_GRAVE);
        if (mvLifecycle.isAbsent() || mvLifecycle.isInactive()) {
            return true;
        }
        return MVLifecycle.whenMVPresentThen(mvLifecycle,
                (lifecycle, db, mv) -> policy.getUnsatisfactoryPerformanceDictator()
                        .apply(mvLifecycle, policy.getMVHitRatioProvider()));
    });

    public static final TransferGuard GRAVE_TO_EXTINCTION_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_GRAVE_TO_EXTINCTION);
        if (mvLifecycle.isAbsent()) {
            return true;
        }

        if (mvLifecycle.isActive()) {
            return false;
        }

        return policy.getExceedMaximumReviveWaitingTimeDictator().test(mvLifecycle);
    });

    public static final TransferGuard GRAVE_TO_CRADLE_GUARD = ((mvLifecycle, transfer, policy) -> {
        Preconditions.checkArgument(transfer == MVPhaseTransfer.MPT_GRAVE_TO_CRADLE);
        return mvLifecycle.isActive();
    });

}
