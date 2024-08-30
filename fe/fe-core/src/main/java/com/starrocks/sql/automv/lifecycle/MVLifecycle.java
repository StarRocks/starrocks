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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.automv.generator.MVName;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.automv.util.Util;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import javax.annotation.Nullable;

public class MVLifecycle {
    @Nullable
    private MaterializedViewPlus mvPlus;
    private MVChangeLog mvChangeLog;

    public MVLifecycle(@Nullable MaterializedViewPlus mvPlus, MVChangeLog mvChangeLog) {
        this.mvPlus = mvPlus;
        this.mvChangeLog = Objects.requireNonNull(mvChangeLog);
    }

    public static MVLifecycle ofAttached(MaterializedViewPlus mvPlus, MVChangeLog mvChangeLog) {
        return new MVLifecycle(Objects.requireNonNull(mvPlus), mvChangeLog);
    }

    public static MVLifecycle ofDangling(MVChangeLog mvChangeLog) {
        return new MVLifecycle(null, mvChangeLog);
    }

    public static MVLifecycle ofCradle(MaterializedViewPlus mvPlus, MVName mvName) {
        return ofAttached(mvPlus, MVChangeLog.genesis(mvName).addNewEntry(MVPhase.MP_CRADLE));
    }

    public static boolean whenMVPresentThen(MVLifecycle mvLifecycle, MVLifecyclePredicate predicate) {
        return mvLifecycle.getMVPlus().map(mvPlus -> MetaUtil.getDatabase(mvPlus.getFqName())
                        .unwrap()
                        .map(db -> GlobalStateMgr.getCurrentState().getLocalMetastore()
                                .mayGetTable(db.getId(), mvPlus.getMv().getId())
                                .map(tbl -> predicate.test(mvLifecycle, db, mvPlus.getMv()))
                                .orElse(false))
                        .orElse(false))
                .orElse(false);
    }

    public MVLifecycle attach(MaterializedViewPlus mv) {
        Preconditions.checkArgument(this.mvPlus == null || this.mvPlus.getFqName().equals(mv.getFqName()));
        this.mvPlus = Objects.requireNonNull(mv);
        return this;
    }

    public long elapsedSeconds() {
        return Util.timeDiff(System.currentTimeMillis(), getEnterTime());
    }

    public MVLifecycle detach() {
        this.mvPlus = null;
        return this;
    }

    public MaterializedViewPlus mustGetMVPlus() {
        return Objects.requireNonNull(getMVPlus().orElse(null));
    }

    public Optional<MaterializedViewPlus> getMVPlus() {
        return Optional.ofNullable(mvPlus);
    }

    public boolean isDetached() {
        return !isAttached();
    }

    public boolean isAttached() {
        return getMVPlus().isPresent();
    }

    public String getDigest() {
        return mvChangeLog.getMVName().getDigest();
    }

    public MVPhase getPhase() {
        return mvChangeLog.getLatestEntry().getPhase();
    }

    public long getEnterTime() {
        return mvChangeLog.getLatestEntry().getEnterTime();
    }

    public MVPhase getPrevPhase() {
        return mvChangeLog.getLatestEntry().getPrevPhase();
    }

    public MVName getMVName() {
        return mvChangeLog.getMVName();
    }

    public MVChangeLog getMVChangeLog() {
        return mvChangeLog;
    }

    public MVLifecycle replaceMVChangeLog(MVLifecycle other) {
        this.mvChangeLog = Objects.requireNonNull(other).mvChangeLog;
        return this;
    }

    public void commit(MVPhase mvPhase) {
        if (mvChangeLog.getEntries().isEmpty() || mvChangeLog.getLatestEntry().getPhase() != mvPhase) {
            mvChangeLog = mvChangeLog.addNewEntry(mvPhase);
            mvChangeLog.persist();
        }
    }

    public boolean isActive() {
        return whenMVPresentThen(this, (lifecycle, db, mv) -> lifecycle.mustGetMVPlus().getMv().isActive());
    }

    public boolean isInactive() {
        return whenMVPresentThen(this, (lifecycle, db, mv) -> !lifecycle.mustGetMVPlus().getMv().isActive());
    }

    public boolean refreshState(Predicate<Collection<Partition>> partitionsPredicate) {
        return whenMVPresentThen(this, (lifecycle, db, mv) ->
                MetaUtil.criticalRegion(db, mv, LockType.READ,
                        () -> partitionsPredicate.test(mv.getPartitions())).unwrap().orElse(false));
    }

    public boolean hasRefreshed() {
        return refreshState(partitions -> partitions.stream().anyMatch(Partition::hasStorageData));
    }

    public boolean neverRefreshed() {
        return refreshState(partitions -> partitions.stream().noneMatch(Partition::hasStorageData));
    }

    public boolean isPresent() {
        return whenMVPresentThen(this, (a, b, c) -> true);
    }

    public boolean isAbsent() {
        return !isPresent();
    }

    public boolean passInternship(MVPhasePolicy policy) {
        return whenMVPresentThen(this,
                (lifecycle, db, mv) -> policy.getInternshipPeriodEndedDictator().test(lifecycle));
    }

    @FunctionalInterface
    public interface MVLifecyclePredicate {
        boolean test(MVLifecycle mvLifecycle, Database db, MaterializedView mv);
    }
}