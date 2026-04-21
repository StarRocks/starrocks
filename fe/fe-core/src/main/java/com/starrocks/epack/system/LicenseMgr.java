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
package com.starrocks.epack.system;

import com.google.gson.Gson;
import com.starrocks.common.CloseableLock;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.common.util.MachineInfo;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.epack.persist.EditLogEPack;
import com.starrocks.epack.persist.RegisterLicenseLog;
import com.starrocks.epack.persist.SRMetaBlockIDEPack;
import com.starrocks.epack.persist.ScaleOutLicenseFreeStartTimeLog;
import com.starrocks.epack.security.SecurityUtils;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.sql.ast.StatementBase;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class LicenseMgr {
    public static final Logger LOG = LogManager.getLogger(LicenseMgr.class);

    // Obfuscated AES key using XOR encoding
    private static final byte[] AES_KEY_ENCODED = {
        0x02, 0x42, 0x0D, 0x10, 0x5E, 0x4C, 0x48, 0x18,
        0x00, 0x0A, 0x24, 0x5B, 0x31, 0x10, 0x43, 0x4F
    };
    private static final byte AES_XOR_KEY = 0x7A;
    
    // Obfuscated RSA public key using base64 encoding and XOR
    private final String rsaKeyEncoded;
    private static final byte RSA_XOR_KEY = 0x3F;

    private static final long FREE_TRIAL_EXPIRE_MS = 1000L * 3600 * 24 * 7; // 7 days
    private static final long LICENSE_TIP_INTERVAL_MS = 1000L * 3600 * 24 * 30; // 30 days
    private static final long SCALE_OUT_LICENSE_FREE_INTERVAL_MS = 1000L * 3600 * 24 * 7; // 7 days

    private FrontendDaemon licenseChecker;
    protected final List<String> licenseList = new ArrayList<>();
    protected SystemInfo systemInfo;
    private volatile Long scaleOutLicenseFreeStartTime;
    private volatile Long scaleOutStartTime;
    private volatile Long scaleOutStartTotalCpuCores;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final AtomicBoolean hasValidLicense = new AtomicBoolean(false);
    private final AtomicBoolean licenseVerified = new AtomicBoolean(false);
    private Clock clock;
    private final NodeMgr nodeMgr;

    public LicenseMgr(NodeMgr nodeMgr) {
        this(Clock.systemDefaultZone(), nodeMgr, "c2wPS3NsDnxtalt1a1Z9bmlqdXJsanJYbA9pZXNsD0tzbA90a2pTdW5UU05ua" +
                "gp8ZQ1LR154S09tRVMMcnp1fWpqaXhuanlvbg95bXB6eXFsalN8bg1bc24PeW1tankMXmhTb25qCnBcRXIOWndtUGlUakVlRWlm" +
                "Xm5PCl1US3RxDXVPcntyRWZndVBuDVtKW3oOTnJSWA5uVXV1clNbVltVfXZwZ21waVF5TGtSbXNyDQpnamlcDV5UeXBtDFdHXnd" +
                "pU2VnUAxld24KXVFbTHxWS2ptUlt0alRtSmxqV2dtUWZIbmhLcGtTeWprem1qXlQHTXBnW0VdeXVzbHpXSmtTZXdsDW1oclRYD3" +
                "J7aXJcVHlMaQxpUm15V2tpelNGaWtbR3BqdnRuD3VvXlVbZlpTZWlqakdKalFTeWoMZXhxVA8PbWtxV2trcWxyaHlUamp1e1x7a" +
                "XZleFMKWngHD2tST1dua3FNXHgPDl1FW3Zye3VJcHtySWtoekhbfk9OcQ9uTXJSaVNxDVd9cQ11V25WS0xmVG5IaXlPaWoPSw9r" +
                "DQ5pXXl1T1pUCk5re21pW3tbVltpfW1oUVdmcHtUD11UR1Jyeml7bA0GZV1pUw5od3F9fFJPS213bW9rVVdQcVVxbVxFegtyRVt" +
                "WW2l9Xl1FdVZcRXEPaVRLDVp7U2lceXV6a3xMSWV6aXRdDHFHZlR1DmpFbX1yU1dRXnpTZWt4U1RyU1t4XmlmdF1RW3Vtenltbm" +
                "p2dHNsD0tzbA55a1RuWGp5aXxrelN7dnpLeWhsD0tzbA9LfFgCAg==");
    }

    protected LicenseMgr(Clock clock, NodeMgr nodeMgr, String rsaKeyEncoded) {
        this.clock = clock;
        this.nodeMgr = nodeMgr;
        this.rsaKeyEncoded = rsaKeyEncoded;
        licenseChecker = new FrontendDaemon("license-checker", 60L * 1000L) {
            @Override
            protected void runAfterCatalogReady() {
                if (systemInfo == null) {
                    initSystemInfo();
                }
        
                verifyAllLicenses();
            }
        };
    }

    public void start() {
        if (LicenseToggle.isEnabled) {
            licenseChecker.start();
        }
    }

    // Obfuscated key retrieval methods
    protected static String getAesKey() {
        // Decode the XOR-encoded AES key
        byte[] decryptedBytes = new byte[AES_KEY_ENCODED.length];
        for (int i = 0; i < AES_KEY_ENCODED.length; i++) {
            decryptedBytes[i] = (byte) (AES_KEY_ENCODED[i] ^ AES_XOR_KEY);
        }
        return new String(decryptedBytes, StandardCharsets.UTF_8);
    }

    protected String getRsaPubKey() {
        // Decode the XOR-encoded RSA key
        byte[] encodedBytes = Base64.getDecoder().decode(rsaKeyEncoded);
        byte[] decodedBytes = new byte[encodedBytes.length];
        for (int i = 0; i < encodedBytes.length; i++) {
            decodedBytes[i] = (byte) (encodedBytes[i] ^ RSA_XOR_KEY);
        }
        return new String(base64Decode(new String(decodedBytes)));
    }

    protected boolean hasValidLicense() {
        // If licenses are not verified yet, we assume it is valid
        return !licenseVerified.get() || hasValidLicense.get() || inFreeTrialPeriod() || inScaleOutLicenseFreePeriod();
    }

    public void checkLicense(StatementBase parseStmt) throws InvalidLicenseException {
        if (!LicenseToggle.isEnabled) {
            return;
        }

        if (!hasValidLicense()
                && (parseStmt.getClass().getSimpleName().startsWith("Create"))) {
            throw new InvalidLicenseException(ErrorCode.ERR_INVALID_LICENSE.formatErrorMsg());
        }
    }

    public void resetSystemInfoIfMachineChanged() {
        if (!LicenseToggle.isEnabled) {
            return;
        }

        String macAddress = MachineInfo.getInstance().getMacAddress();
        if (macAddress == null) {
            return;
        }

        Set<String> allFENodesMacAddress = nodeMgr.getAllFENodesMacAddress();
        if (allFENodesMacAddress.isEmpty() || allFENodesMacAddress.contains(macAddress)) {
            return;
        }

        // reset systemInfo
        initSystemInfo();
    }

    protected void verifyAllLicenses() {
        long currentTotalCpuCores = nodeMgr == null ? 0L : nodeMgr.getTotalCpuCores();
        long currentTimeMillis = clock.millis();
        long maxExpire = 0;
        boolean foundValidLicense = false;
        boolean foundCoreOverageInvalidLicense = false;
        try (CloseableLock ignored = CloseableLock.lock(this.lock.readLock())) {
            for (String license : licenseList) {
                try {
                    LicenseInfo licenseInfo = verifyLicense(license);

                    // Keep the invalidation reason granular here so scale-out grace is
                    // opened only for post-scale-out core overage, not for unrelated
                    // invalid licenses such as expiry or systemID mismatch.
                    if (!systemInfo.getSystemID().equals(licenseInfo.getSystemID())) {
                        continue;
                    }
                    if (currentTimeMillis > licenseInfo.getExpire()) {
                        continue;
                    }
                    if (currentTotalCpuCores > licenseInfo.getCores()) {
                        foundCoreOverageInvalidLicense = true;
                        continue;
                    }

                    maxExpire = Math.max(maxExpire, licenseInfo.getExpire());
                    foundValidLicense = true;
                } catch (InvalidLicenseException ignored1) {
                }
            }
        }

        hasValidLicense.set(foundValidLicense);
        processScaleOutStateAfterVerify(currentTotalCpuCores, foundValidLicense, foundCoreOverageInvalidLicense);

        if (!hasValidLicense.get()) {
            if (inFreeTrialPeriod()) {
                LOG.warn("Please add a valid license, Creating objects will be rejected after {}",
                        DateUtils.formatTimestampInSeconds(((systemInfo.getBuildTime() + FREE_TRIAL_EXPIRE_MS) / 1000)));
            } else if (inScaleOutLicenseFreePeriod()) {
                LOG.warn("No valid license found, cluster is in scale-out grace period until {}",
                        DateUtils.formatTimestampInSeconds(
                                (getScaleOutLicenseFreeDeadline() / 1000)));
            } else {
                LOG.warn("No valid license found, please add a valid license.");
            }
        }

        if (maxExpire > 0 && maxExpire - clock.millis() < LICENSE_TIP_INTERVAL_MS) {
            LOG.warn("License will expire at {}, please add a new license.",
                    DateUtils.formatTimestampInSeconds(maxExpire / 1000));
        }

        licenseVerified.set(true);
    }

    public List<LicenseInfo> getAllLicenseInfo() {
        List<LicenseInfo> licenseInfos = new ArrayList<>();
        try (CloseableLock ignored = CloseableLock.lock(this.lock.readLock())) {
            for (String license : licenseList) {
                try {
                    LicenseInfo info = verifyLicense(license);

                    verifyLicenseInfo(info);

                    licenseInfos.add(info);
                } catch (InvalidLicenseException e) {
                    // skip invalid license
                }
            }
        }
        if (inFreeTrialPeriod() && licenseInfos.isEmpty()) {
            licenseInfos.add(new LicenseInfo("", systemInfo.getSystemID(), nodeMgr.getTotalCpuCores(),
                    systemInfo.getBuildTime() + FREE_TRIAL_EXPIRE_MS));
        }
        return licenseInfos;
    }

    public long getMaxCpuCores() {
        long maxCpuCores = 0;
        try (CloseableLock ignored = CloseableLock.lock(this.lock.readLock())) {
            for (String license : licenseList) {
                try {
                    LicenseInfo info = verifyLicense(license);

                    verifyLicenseInfo(info);

                    maxCpuCores = Math.max(maxCpuCores, info.getCores());
                } catch (InvalidLicenseException e) {
                    // skip invalid license
                }
            }
        }
        return maxCpuCores;
    }

    public long getLicenseExpireDays() {
        if (systemInfo == null) {
            return 0L;
        }

        long maxExpire = 0;
        try (CloseableLock ignored = CloseableLock.lock(this.lock.readLock())) {
            for (String license : licenseList) {
                try {
                    LicenseInfo info = verifyLicense(license);
                    verifyLicenseInfo(info);
                    maxExpire = Math.max(maxExpire, info.getExpire());
                } catch (InvalidLicenseException e) {
                    // skip invalid license
                }
            }
        }

        if (maxExpire <= 0 && inFreeTrialPeriod()) {
            maxExpire = systemInfo.getBuildTime() + FREE_TRIAL_EXPIRE_MS;
        }

        if (maxExpire <= 0 && inScaleOutLicenseFreePeriod()) {
            maxExpire = getScaleOutLicenseFreeDeadline();
        }

        if (maxExpire <= 0) {
            return 0L;
        }

        long remainingMillis = Math.max(0L, maxExpire - clock.millis());
        return TimeUnit.MILLISECONDS.toDays(remainingMillis);
    }

    protected boolean inFreeTrialPeriod() {
        if (systemInfo == null) {
            return true;
        }

        long currentTime = clock.millis();
        return (currentTime - systemInfo.getBuildTime()) < FREE_TRIAL_EXPIRE_MS;
    }

    protected boolean inScaleOutLicenseFreePeriod() {
        if (scaleOutLicenseFreeStartTime == null) {
            return false;
        }
        return clock.millis() < getScaleOutLicenseFreeDeadline();
    }

    protected Long getScaleOutLicenseFreeStartTime() {
        return scaleOutLicenseFreeStartTime;
    }

    protected Long getScaleOutStartTime() {
        return scaleOutStartTime;
    }

    protected Long getScaleOutStartTotalCpuCores() {
        return scaleOutStartTotalCpuCores;
    }

    private long getScaleOutLicenseFreeDeadline() {
        return scaleOutLicenseFreeStartTime + SCALE_OUT_LICENSE_FREE_INTERVAL_MS;
    }

    private void initSystemInfo() {
        SystemInfo info = new SystemInfo(UUIDUtil.genUUID().toString(), clock.millis());

        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logInitSystemInfo(info, wal -> applyInitSystemInfo((SystemInfo) wal));
    }

    public void applyInitSystemInfo(SystemInfo systemInfo) {
        this.systemInfo = systemInfo;
    }

    public void registerLicense(String license) throws InvalidLicenseException {
        if (hasSameLicense(license)) {
            return;
        }

        LicenseInfo licenseInfo = verifyLicense(license);
        verifyLicenseInfo(licenseInfo);

        // update hasValidLicense only on leader node.
        hasValidLicense.set(true);
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logRegisterLicense(new RegisterLicenseLog(license, null),
                wal -> applyRegisterLicense((RegisterLicenseLog) wal));
    }

    private boolean hasSameLicense(String license) {
        try (CloseableLock ignored = CloseableLock.lock(this.lock.readLock())) {
            for (String l : licenseList) {
                if (l.equals(license)) {
                    return true;
                }
            }
        }
        return false;
    }

    public void applyRegisterLicense(RegisterLicenseLog log) {
        try (CloseableLock ignored = CloseableLock.lock(this.lock.writeLock())) {
            licenseList.add(log.getLicense());
            scaleOutLicenseFreeStartTime = null;
            scaleOutStartTime = null;
            scaleOutStartTotalCpuCores = null;
        }
    }

    public void recordScaleOutStart() {
        if (scaleOutStartTime != null) {
            return;
        }
        setScaleOutStart(clock.millis(), nodeMgr.getTotalCpuCores());
    }

    protected void setScaleOutStart(Long startTime, Long totalCpuCores) {
        scaleOutStartTime = startTime;
        scaleOutStartTotalCpuCores = totalCpuCores;
    }

    public void applyScaleOutLicenseFreeStartTime(ScaleOutLicenseFreeStartTimeLog log) {
        scaleOutLicenseFreeStartTime = log.getScaleOutLicenseFreeStartTime();
    }

    private void clearScaleOutStart() {
        if (scaleOutStartTime == null && scaleOutStartTotalCpuCores == null) {
            return;
        }
        setScaleOutStart(null, null);
    }

    private void updateScaleOutLicenseFreeStartTime(Long newScaleOutLicenseFreeStartTime) {
        if (scaleOutLicenseFreeStartTime != null
                && scaleOutLicenseFreeStartTime.equals(newScaleOutLicenseFreeStartTime)) {
            return;
        }

        ScaleOutLicenseFreeStartTimeLog log =
                new ScaleOutLicenseFreeStartTimeLog(newScaleOutLicenseFreeStartTime);
        EditLogEPack editLogEPack = (EditLogEPack) GlobalStateMgr.getCurrentState().getEditLog();
        editLogEPack.logUpdateScaleOutLicenseFreeStartTime(log,
                wal -> applyScaleOutLicenseFreeStartTime((ScaleOutLicenseFreeStartTimeLog) wal));
    }

    private void processScaleOutStateAfterVerify(long currentTotalCpuCores,
                                                 boolean foundValidLicense,
                                                 boolean foundCoreOverageInvalidLicense) {
        // No pending scale-out means there is nothing to convert into a grace period.
        if (scaleOutStartTime == null || scaleOutStartTotalCpuCores == null) {
            return;
        }

        // We only start the grace period after the checker observes that the cluster
        // really grew beyond the baseline captured when scale-out started.
        if (currentTotalCpuCores <= scaleOutStartTotalCpuCores) {
            return;
        }

        // Grace is reserved for the specific case where scale-out made CPU cores exceed
        // the available license coverage. If another license still covers the current
        // cluster, or the cluster is invalid for an unrelated reason such as
        // expiry/systemID mismatch, we just consume the pending marker.
        if (foundValidLicense || !foundCoreOverageInvalidLicense || scaleOutLicenseFreeStartTime != null) {
            clearScaleOutStart();
            return;
        }

        // The cluster actually grew and is now out of license, so the pending
        // scale-out start becomes the beginning of the 7-day grace window.
        updateScaleOutLicenseFreeStartTime(scaleOutStartTime);
        clearScaleOutStart();
    }

    public String getEncryptedLicenseInfo() throws Exception {
        Gson gson = new Gson();
        String json = gson.toJson(new LicenseInfo(systemInfo.getSystemID()));
        byte[] encrypted = SecurityUtils.aes128Encrypt(str2Bytes(json), str2Bytes(getAesKey()));
        return Base64.getEncoder().encodeToString(encrypted);
    }

    protected void verifyLicenseInfo(LicenseInfo licenseInfo) throws InvalidLicenseException {
        if (!systemInfo.getSystemID().equals(licenseInfo.getSystemID())) {
            throw new InvalidLicenseException("systemID not equal, current: %s, license: %s",
                    systemInfo.getSystemID(), licenseInfo.getSystemID());
        }

        if (nodeMgr.getTotalCpuCores() > licenseInfo.getCores()) {
            throw new InvalidLicenseException("cpu cores not enough, current: %d, license: %d",
                    nodeMgr.getTotalCpuCores(), licenseInfo.getCores());
        }

        long currentTimeMillis = clock.millis();
        if (currentTimeMillis > licenseInfo.getExpire()) {
            throw new InvalidLicenseException("license expired, current time: %d, expire time: %d",
                    currentTimeMillis, licenseInfo.getExpire());
        }
    }

    protected LicenseInfo verifyLicense(String l)
            throws InvalidLicenseException {
        // 1. Base64 decode
        byte[] license = Base64.getDecoder().decode(l);

        // 2. AES decrypt
        byte[] licenseDecrypted;
        try {
            licenseDecrypted = SecurityUtils.aes128Decrypt(license, str2Bytes(getAesKey()));
        } catch (Exception e) {
            LOG.warn("Failed to decrypt license", e);
            throw new InvalidLicenseException("Failed to decrypt license.");
        }

        // 3. deserialize JSON
        Gson gson = new Gson();
        LicenseInfo licenseInfo = gson.fromJson(bytes2Str(licenseDecrypted), LicenseInfo.class);
        if (licenseInfo.getSign() == null || licenseInfo.getSign().isEmpty()) {
            throw new InvalidLicenseException("Failed to parse license.");
        }
        String sign = licenseInfo.getSign();

        // 4. serialize licenseInfo without sign
        licenseInfo.setSign("");
        byte[] jData = str2Bytes(gson.toJson(licenseInfo));

        // 5. Base64 decode sign
        byte[] signData = Base64.getDecoder().decode(sign);

        // 6. verify
        boolean verifyResult = false;
        try {
            verifyResult = SecurityUtils.rsaVerifySign(jData, signData, str2Bytes(getRsaPubKey()));
        } catch (Exception e) {
            LOG.warn("Failed to verify license signature", e);
            throw new InvalidLicenseException("Failed to verify license signature.");
        }
        if (!verifyResult) {
            throw new InvalidLicenseException("License signature verification failed.");
        }

        return licenseInfo;
    }

    public boolean checkLicenseForAddBackend(String host) throws InvalidLicenseException {
        return checkLicenseForScaleOut(host, "backend/computeNode");
    }

    public boolean checkLicenseForAddFrontend(String host) throws InvalidLicenseException {
        return checkLicenseForScaleOut(host, "frontend");
    }

    private boolean checkLicenseForScaleOut(String host, String nodeType) throws InvalidLicenseException {
        if (!LicenseToggle.isEnabled) {
            return false;
        }

        // Duplicate-host expansion, free trial, and an already-open grace period
        // should all bypass the "record pending scale-out start" path.
        if (nodeMgr.getAllNodeHosts().contains(host) || inFreeTrialPeriod() || inScaleOutLicenseFreePeriod()) {
            return false;
        }

        verifyAllLicenses();
        // verifyAllLicenses() may open the grace period when it observes that a prior
        // pending scale-out has already increased total CPU cores.
        if (inScaleOutLicenseFreePeriod()) {
            return false;
        }
        if (!hasValidLicense()) {
            throw new InvalidLicenseException("current license is invalid, adding %s is not allowed", nodeType);
        }

        // Only the first scale-out request while license is still valid should
        // record the pending baseline for the periodic checker.
        return scaleOutStartTime == null;
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockIDEPack.LICENSE_MGR, 1);

        String systemInfoStr = GsonUtils.GSON.toJson(systemInfo);
        byte[] encryptedSystemInfo = null;
        try {
            encryptedSystemInfo = SecurityUtils.aes128Encrypt(str2Bytes(systemInfoStr), str2Bytes(getAesKey()));
        } catch (Exception e) {
            LOG.warn("Failed to encrypt system info, use plain text to store", e);
        }

        if (encryptedSystemInfo == null) {
            writer.writeJson(new LicenseMgrPersist(licenseList, false, base64Encode(systemInfoStr),
                    scaleOutLicenseFreeStartTime));
        } else {
            writer.writeJson(new LicenseMgrPersist(licenseList, true, base64Encode(encryptedSystemInfo),
                    scaleOutLicenseFreeStartTime));
        }
        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        LicenseMgrPersist licenseMgrPersist = reader.readJson(LicenseMgrPersist.class);
        licenseList.addAll(licenseMgrPersist.licenses);
        scaleOutLicenseFreeStartTime = licenseMgrPersist.scaleOutLicenseFreeStartTime;
        if (licenseMgrPersist.isEncrypted) {
            try {
                byte[] systemInfoBytes = SecurityUtils.aes128Decrypt(base64Decode(licenseMgrPersist.systemInfoStr),
                        str2Bytes(getAesKey()));
                systemInfo = GsonUtils.GSON.fromJson(bytes2Str(systemInfoBytes), SystemInfo.class);
            } catch (Exception e) {
                LOG.warn("Failed to decrypt system info", e);
                throw new IOException("Failed to decrypt system info", e);
            }
        } else {
            String systemInfoStr = bytes2Str(base64Decode(licenseMgrPersist.systemInfoStr));
            systemInfo = GsonUtils.GSON.fromJson(systemInfoStr, SystemInfo.class);
        }
    }

    private static byte[] str2Bytes(String str) {
        return str.getBytes(StandardCharsets.UTF_8);
    }

    private static String bytes2Str(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String base64Encode(String src) {
        return Base64.getEncoder().encodeToString(str2Bytes(src));
    }

    private static String base64Encode(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] base64Decode(String src) {
        return Base64.getDecoder().decode(src);
    }

    // For test
    protected void setClock(Clock clock) {
        this.clock = clock;
    }
}
