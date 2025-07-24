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

package com.starrocks.common.util;

import com.google.common.base.Strings;
import com.starrocks.service.FrontendOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

public class MachineInfo {
    private static final Logger LOG = LogManager.getLogger(MachineInfo.class);

    private MachineInfo() {}
    private static class Holder {
        private static final MachineInfo INSTANCE = new MachineInfo();
    }
    public static MachineInfo getInstance() {
        return Holder.INSTANCE;
    }

    private volatile Integer cpuCores = null;
    private volatile String macAddress = null;

    public int getCpuCores() {
        if (cpuCores == null) {
            synchronized (this) {
                if (cpuCores == null) {
                    cpuCores = computeCpuCores();
                }
            }
        }
        return cpuCores;
    }

    public String getMacAddress() {
        if (macAddress == null) {
            synchronized (this) {
                if (macAddress == null) {
                    macAddress = computeMacAddress();
                }
            }
        }
        return macAddress;
    }

    private int computeCpuCores() {
        int hostCores = 0;
        try (BufferedReader reader = new BufferedReader(new FileReader("/proc/cpuinfo"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                int colon = line.indexOf(':');
                if (colon != -1) {
                    String name = line.substring(0, colon).trim();
                    if (name.equals("processor")) {
                        hostCores++;
                    }
                }
            }
        } catch (IOException ignored) {
        }
        return getCgroupCpuLimit(hostCores);
    }

    private int getCgroupCpuLimit(int defaultCores) {
        // If not in a Docker container, return the default cores
        if (!Files.exists(Paths.get("/.dockerenv"))) {
            return defaultCores;
        }
        int cfsNumCores = defaultCores;
        int cpusetNumCores = defaultCores;
        try {
            String fsType = getFsType("/sys/fs/cgroup");
            String cfsPeriodStr = null;
            String cfsQuotaStr = null;
            String cpusetStr = null;
            // Determine the cgroup filesystem type and read the appropriate files
            if ("tmpfs".equals(fsType)) {
                cfsPeriodStr = readFileTrim("/sys/fs/cgroup/cpu/cpu.cfs_period_us");
                cfsQuotaStr = readFileTrim("/sys/fs/cgroup/cpu/cpu.cfs_quota_us");
                cpusetStr = readFileTrim("/sys/fs/cgroup/cpuset/cpuset.cpus");
            } else if ("cgroup2".equals(fsType)) {
                String cpuMax = readFileTrim("/sys/fs/cgroup/cpu.max");
                if (!Strings.isNullOrEmpty(cpuMax)) {
                    String[] parts = cpuMax.split("\\s+");
                    if (parts.length >= 2) {
                        cfsQuotaStr = parts[0];
                        cfsPeriodStr = parts[1];
                    }
                }
                cpusetStr = readFileTrim("/sys/fs/cgroup/cpuset.cpus");
            }
            // Parse the cgroup files to determine CPU limits
            if (cfsPeriodStr != null && cfsQuotaStr != null) {
                try {
                    int cfsPeriod = Integer.parseInt(cfsPeriodStr.trim());
                    int cfsQuota = Integer.parseInt(cfsQuotaStr.trim());
                    if (cfsQuota > 0 && cfsPeriod > 0) {
                        cfsNumCores = cfsQuota / cfsPeriod;
                    }
                } catch (NumberFormatException ignored) {
                }
            }
            // Parse the cpuset.cpus file to determine available CPU cores
            if (!Strings.isNullOrEmpty(cpusetStr)) {
                Set<Integer> cpusetCores = parseCpusetCpus(cpusetStr);
                cpusetCores.removeAll(getOfflineCores());
                cpusetNumCores = cpusetCores.size();
            }
        } catch (Exception ignored) {
        }
        // Ensure the number of cores is at least 1 and get the minimum of the two limits
        return Math.max(1, Math.min(cfsNumCores, cpusetNumCores));
    }

    private String getFsType(String path) throws Exception {
        Path cgroupPath = Paths.get(path);
        FileStore store = Files.getFileStore(cgroupPath);
        return store.type();
    }

    private String readFileTrim(String path) {
        try {
            return Files.readString(Paths.get(path)).trim();
        } catch (Exception e) {
            return null;
        }
    }

    private Set<Integer> parseCpusetCpus(String cpusStr) {
        Set<Integer> cpuids = new HashSet<>();
        for (String part : cpusStr.split(",")) {
            part = part.trim();
            if (part.contains("-")) {
                String[] range = part.split("-");
                if (range.length == 2) {
                    int start = Integer.parseInt(range[0].trim());
                    int end = Integer.parseInt(range[1].trim());
                    for (int i = start; i <= end; i++) {
                        cpuids.add(i);
                    }
                }
            } else if (!part.isEmpty()) {
                cpuids.add(Integer.parseInt(part));
            }
        }
        return cpuids;
    }

    private Set<Integer> getOfflineCores() {
        Set<Integer> offline = new HashSet<>();
        String offlineStr = readFileTrim("/sys/devices/system/cpu/offline");
        if (offlineStr != null && !offlineStr.isEmpty()) {
            offline.addAll(parseCpusetCpus(offlineStr));
        }
        return offline;
    }

    private String computeMacAddress() {
        if (Files.exists(Paths.get("/.dockerenv"))) {
            return "unknown";
        }
        InetAddress address = FrontendOptions.getLocalAddr();
        try {
            NetworkInterface networkInterface = NetworkInterface.getByInetAddress(address);
            if (networkInterface == null) {
                LOG.warn("cannot find network interface by host {}", address.getHostAddress());
                return "unknown";
            }
            byte[] macAddressBytes = networkInterface.getHardwareAddress();
            if (macAddressBytes == null) {
                LOG.warn("cannot get mac address of network interface {}", networkInterface.getName());
                return "unknown";
            }
            StringBuilder macAddressBuilder = new StringBuilder();
            for (int i = 0; i < macAddressBytes.length; i++) {
                macAddressBuilder.append(String.format("%02X%s", macAddressBytes[i],
                        (i < macAddressBytes.length - 1) ? "-" : ""));
            }
            return macAddressBuilder.toString();
        } catch (Exception e) {
            LOG.warn("get mac address failed", e);
            return "unknown";
        }
    }
} 