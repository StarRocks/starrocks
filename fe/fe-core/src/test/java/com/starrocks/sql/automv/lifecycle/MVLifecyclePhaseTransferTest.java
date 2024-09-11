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

import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.scheduler.MVLifecycleAutoKeeper;
import com.starrocks.sql.automv.tunespace.MaterializedViewPlus;
import com.starrocks.sql.automv.util.AutoMVUtil;
import com.starrocks.sql.automv.util.MetaUtil;
import com.starrocks.sql.automv.util.TestUtil;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class MVLifecyclePhaseTransferTest {
    private static final ThreadLocal<StarRocksAssert> STARROCKS_ASSERT = new ThreadLocal<>();

    private static StarRocksAssert getStarRocksAssert() {
        FeConstants.runningUnitTest = true;
        if (STARROCKS_ASSERT.get() == null) {
            STARROCKS_ASSERT.set(TestUtil.prepareTables("ssb", TestUtil::getSsbCreateTableSqlList));
        }
        return STARROCKS_ASSERT.get();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        setUpMock();
    }

    private static void setUpMock() {
        FeConstants.runningUnitTest = true;
        StarRocksAssert starRocksAssert = getStarRocksAssert();
        AutoMVUtil.mockUpTunespaceExecutor();
        AutoMVUtil.mockUpAuthorizer();
        UtFrameUtils.setDefaultConfigForAsyncMVTest(starRocksAssert.getCtx());
        if (!starRocksAssert.databaseExist("_statistics_")) {
            StatisticsMetaManager m = new StatisticsMetaManager();
            m.createStatisticsTablesForTest();
        }
        UtFrameUtils.mockTimelinessForAsyncMVTest(starRocksAssert.getCtx());
    }

    void mockHasRefreshed(boolean result) {
        new MockUp<MVLifecycle>() {
            @Mock
            public boolean hasRefreshed() {
                return result;
            }
        };
    }

    @Test
    public void testCradleToIntern() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        mockHasRefreshed(false);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);
        env.cleanup();
    }

    @Test
    public void testCradleToGrave() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mvLifecycle.mustGetMVPlus().getMv().setInactiveAndReason("inefficient MV");
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);
        env.cleanup();
    }

    @Test
    public void testCradleToExtinction1() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        MetaUtil.dropMV(mvLifecycle.mustGetMVPlus().getFqName().toSql());
        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInfantAbortionDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();
        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleAutoKeeper().process(getStarRocksAssert().getCtx(), () -> true);
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_EXTINCTION);
        env.cleanup();
    }

    @Test
    public void testCradleToExtinction2() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        MetaUtil.dropDb(mvLifecycle.mustGetMVPlus().getFqName().getDb());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_EXTINCTION);
        env.cleanup();
    }

    @Test
    public void testCradleToExtinction3() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        MetaUtil.dropMV(mvLifecycle.mustGetMVPlus().getFqName().toSql());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_EXTINCTION);
        env.cleanup();
    }

    @Test
    public void testInternToTenuredOrRetired() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        AtomicBoolean internshipPeriodEndedFlag = new AtomicBoolean(Boolean.FALSE);
        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> internshipPeriodEndedFlag.get())
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        internshipPeriodEndedFlag.set(true);
        double hitRatioLwm = GlobalVariable.getAutoMVLifecycleHitRatioLwm();
        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), (hitRatioHwm + hitRatioLwm) / 2);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        MVLifecycle savedMVLifecycle = MVLifecycle.ofDangling(mvLifecycle.getMVChangeLog());

        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        mvLifecycle.replaceMVChangeLog(savedMVLifecycle);

        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioLwm - 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_RETIRED);
        env.cleanup();
    }

    @Test
    public void testInternToGrave1() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        MetaUtil.dropDb(mvLifecycle.mustGetMVPlus().getFqName().getDb());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);
        env.cleanup();
    }

    @Test
    public void testInternToGrave2() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        MetaUtil.dropMV(mvLifecycle.mustGetMVPlus().getFqName().toSql());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);
        env.cleanup();
    }

    @Test
    public void testInternToGrave3() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        mvLifecycle.mustGetMVPlus().getMv().setInactiveAndReason("inefficient MV");
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertTrue(mvLifecycle.isInactive());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);
        env.cleanup();
    }

    @Test
    public void testTenuredToRetired() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        policySupplier = () -> MVPhasePolicy.newBuilder()
                .setReachPerformanceEvaluationTimeDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();
        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);

        double hitRatioLwm = GlobalVariable.getAutoMVLifecycleHitRatioLwm();
        mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioLwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);

        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioLwm - 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);

        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_RETIRED);

        env.cleanup();
    }

    @Test
    public void testTenuredToGrave1() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        MetaUtil.dropDb(mvLifecycle.mustGetMVPlus().getFqName().getDb());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);

        env.cleanup();
    }

    @Test
    public void testTenuredToGrave2() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        MetaUtil.dropMV(mvLifecycle.mustGetMVPlus().getFqName().toSql());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);

        env.cleanup();
    }

    @Test
    public void testTenuredToGrave3() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        mvLifecycle.mustGetMVPlus().getMv().setInactiveAndReason("inefficient");
        Assert.assertTrue(mvLifecycle.isPresent() && mvLifecycle.isInactive());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);
        env.cleanup();
    }

    @Test
    public void testGraveToCradle() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        mvLifecycle.mustGetMVPlus().getMv().setInactiveAndReason("inefficient");
        Assert.assertTrue(mvLifecycle.isPresent() && mvLifecycle.isInactive());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);

        mvLifecycle.mustGetMVPlus().getMv().setActive();
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);
        env.cleanup();
    }

    @Test
    public void testGraveToExtinction1() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        mvLifecycle.mustGetMVPlus().getMv().setInactiveAndReason("inefficient");
        Assert.assertTrue(mvLifecycle.isPresent() && mvLifecycle.isInactive());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);

        policySupplier = () -> MVPhasePolicy.newBuilder()
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .setExceedMaximumReviveWaitingTimeDictator(lifecycle -> true)
                .build();
        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAbsent());
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_EXTINCTION);
        env.cleanup();
    }

    @Test
    public void testGraveToExtinction2() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        MetaUtil.dropDb(mvLifecycle.mustGetMVPlus().getFqName().getDb());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);

        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_EXTINCTION);

        env.cleanup();
    }

    @Test
    public void testCleanupExtinction() throws Throwable {
        MVLifecycleEnv env = new MVLifecycleEnv();
        MVLifecycle mvLifecycle = env.getMVLifecycle();

        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_CRADLE);

        mockHasRefreshed(true);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_INTERN);

        Supplier<MVPhasePolicy> policySupplier = () -> MVPhasePolicy.newBuilder()
                .setInternshipPeriodEndedDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();

        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();

        double hitRatioHwm = GlobalVariable.getAutoMVLifecycleHitRatioHwm();
        ConcurrentHashMap<String, Double> mvHitRatioMap = new ConcurrentHashMap<>();
        mvHitRatioMap.put(mvLifecycle.getMVName().toString(), hitRatioHwm + 1);
        env.getMVLifecycleManager().populateMVHitRatio(mvHitRatioMap);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isAttached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_TENURED);

        MetaUtil.dropDb(mvLifecycle.mustGetMVPlus().getFqName().getDb());
        Assert.assertTrue(mvLifecycle.isAbsent());
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(mvLifecycle.isDetached());
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_GRAVE);

        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertSame(mvLifecycle.getPhase(), MVPhase.MP_EXTINCTION);

        Assert.assertFalse(env.getMVLifecycleManager().getNameToMVLifecycles().isEmpty());
        policySupplier = () -> MVPhasePolicy.newBuilder()
                .setExceedExtinctionRetentionMaxTimeDictator(lifecycle -> true)
                .setMVHitRatioProvider(env.getMVLifecycleManager()::getMVHitRatio)
                .build();
        env.getMVLifecycleManager().setMVPhasePolicySupplier(policySupplier);
        env.getMVLifecycleManager().scanMVLifecycles();
        Assert.assertTrue(env.getMVLifecycleManager().getNameToMVLifecycles().isEmpty());
        env.cleanup();
    }

    public static final class MVLifecycleEnv {
        private final MVLifecycleManager mvLifecycleManager;
        private final MVLifecycleAutoKeeper mvLifecycleAutoKeeper;

        public MVLifecycleEnv() {
            List<Pair<String, String>> queryList = TestUtil.getSsbLineorderFlatQueryList()
                    .stream()
                    .filter(p -> p.first.equals("Q1.1"))
                    .collect(Collectors.toList());
            AutoMVUtil.mockUpCustomizedQueryExecutor(queryList, "default_catalog", "ssb");
            mvLifecycleAutoKeeper = new MVLifecycleAutoKeeper();
            mvLifecycleManager = mvLifecycleAutoKeeper.getMVLifecycleManager();
        }

        public MVLifecycleManager getMVLifecycleManager() {
            return mvLifecycleManager;
        }

        public MVLifecycleAutoKeeper getMVLifecycleAutoKeeper() {
            return mvLifecycleAutoKeeper;
        }

        public MVLifecycle getMVLifecycle() throws Throwable {
            GlobalVariable.setAutoMVPerLatticeMVSelectivityRatio(-1.0);
            GlobalVariable.setAutoMVPerLatticeMVLimit(-1);
            mvLifecycleAutoKeeper.process(getStarRocksAssert().getCtx(), () -> true);
            mvLifecycleAutoKeeper.process(getStarRocksAssert().getCtx(), () -> true);
            List<MaterializedViewPlus> mvs = MetaUtil.listLegacyMVs(null, "automv_db");
            Assert.assertEquals(mvs.size(), 1);
            Assert.assertEquals(mvLifecycleManager.getNameToMVLifecycles().size(), 1);
            return mvLifecycleManager.getNameToMVLifecycles().values().iterator().next();
        }

        public void cleanup() {
            MetaUtil.dropDb("automv_db");
        }
    }
}
