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

import static com.starrocks.sql.automv.lifecycle.MVPhase.MP_CRADLE;
import static com.starrocks.sql.automv.lifecycle.MVPhase.MP_EXTINCTION;
import static com.starrocks.sql.automv.lifecycle.MVPhase.MP_GRAVE;
import static com.starrocks.sql.automv.lifecycle.MVPhase.MP_INTERN;
import static com.starrocks.sql.automv.lifecycle.MVPhase.MP_RETIRED;
import static com.starrocks.sql.automv.lifecycle.MVPhase.MP_TENURED;

public enum MVPhaseTransfer {
    MPT_CRADLE_TO_CRADLE(MP_CRADLE, MP_CRADLE),
    MPT_CRADLE_TO_INTERN(MP_CRADLE, MP_INTERN),
    MPT_CRADLE_TO_TENURED(MP_CRADLE, MP_TENURED),
    MPT_CRADLE_TO_RETIRED(MP_CRADLE, MP_RETIRED),
    MPT_CRADLE_TO_GRAVE(MP_CRADLE, MP_GRAVE),
    MPT_CRADLE_TO_EXTINCTION(MP_CRADLE, MP_EXTINCTION),
    MPT_INTERN_TO_CRADLE(MP_INTERN, MP_CRADLE),
    MPT_INTERN_TO_INTERN(MP_INTERN, MP_INTERN),
    MPT_INTERN_TO_TENURED(MP_INTERN, MP_TENURED),
    MPT_INTERN_TO_RETIRED(MP_INTERN, MP_RETIRED),
    MPT_INTERN_TO_GRAVE(MP_INTERN, MP_GRAVE),
    MPT_INTERN_TO_EXTINCTION(MP_INTERN, MP_EXTINCTION),
    MPT_TENURED_TO_CRADLE(MP_TENURED, MP_CRADLE),
    MPT_TENURED_TO_INTERN(MP_TENURED, MP_INTERN),
    MPT_TENURED_TO_TENURED(MP_TENURED, MP_TENURED),
    MPT_TENURED_TO_RETIRED(MP_TENURED, MP_RETIRED),
    MPT_TENURED_TO_GRAVE(MP_TENURED, MP_GRAVE),
    MPT_TENURED_TO_EXTINCTION(MP_TENURED, MP_EXTINCTION),
    MPT_RETIRED_TO_CRADLE(MP_RETIRED, MP_CRADLE),
    MPT_RETIRED_TO_INTERN(MP_RETIRED, MP_INTERN),
    MPT_RETIRED_TO_TENURED(MP_RETIRED, MP_TENURED),
    MPT_RETIRED_TO_RETIRED(MP_RETIRED, MP_RETIRED),
    MPT_RETIRED_TO_GRAVE(MP_RETIRED, MP_GRAVE),
    MPT_RETIRED_TO_EXTINCTION(MP_RETIRED, MP_EXTINCTION),
    MPT_GRAVE_TO_CRADLE(MP_GRAVE, MP_CRADLE),
    MPT_GRAVE_TO_INTERN(MP_GRAVE, MP_INTERN),
    MPT_GRAVE_TO_TENURED(MP_GRAVE, MP_TENURED),
    MPT_GRAVE_TO_RETIRED(MP_GRAVE, MP_RETIRED),
    MPT_GRAVE_TO_GRAVE(MP_GRAVE, MP_GRAVE),
    MPT_GRAVE_TO_EXTINCTION(MP_GRAVE, MP_EXTINCTION),
    MPT_EXTINCTION_TO_CRADLE(MP_EXTINCTION, MP_CRADLE),
    MPT_EXTINCTION_TO_INTERN(MP_EXTINCTION, MP_INTERN),
    MPT_EXTINCTION_TO_TENURED(MP_EXTINCTION, MP_TENURED),
    MPT_EXTINCTION_TO_RETIRED(MP_EXTINCTION, MP_RETIRED),
    MPT_EXTINCTION_TO_GRAVE(MP_EXTINCTION, MP_GRAVE),
    MPT_EXTINCTION_TO_EXTINCTION(MP_EXTINCTION, MP_EXTINCTION);
    private final MVPhase fromPhase;
    private final MVPhase toPhase;

    MVPhaseTransfer(MVPhase fromPhase, MVPhase toPhase) {
        this.fromPhase = fromPhase;
        this.toPhase = toPhase;
    }

    public static MVPhaseTransfer of(MVPhase fromPhase, MVPhase toPhase) {
        int ordinal = fromPhase.ordinal() * MVPhase.values().length + toPhase.ordinal();
        return MVPhaseTransfer.values()[ordinal];
    }

    public MVPhase getFromPhase() {
        return fromPhase;
    }

    public MVPhase getToPhase() {
        return toPhase;
    }
}