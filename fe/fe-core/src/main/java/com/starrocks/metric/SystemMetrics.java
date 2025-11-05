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

package com.starrocks.metric;

import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.service.FrontendOptions;
import org.apache.arrow.util.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Reports system-level metrics, including CPU, memory, I/O, and network statistics.
 *
 * <p><b>Usage Scenario:</b>
 * This class is used within the StarRocks monitoring framework to provide a snapshot of the host's system health.
 * The collected metrics can be exposed through a metrics endpoint for observation and alerting.
 *
 * <p><b>Implementation Mechanism:</b>
 * It acts as a facade, aggregating different metric collectors (for CPU, memory, I/O, and network) into a single component.
 * It initializes and updates these collectors, which in turn use the {@code oshi} library to gather raw system information.
 */
public class SystemMetrics {
    private static final Logger LOG = LogManager.getLogger(SystemMetrics.class);

    private final SystemInfo systemInfo;
    private final StarRocksMetricRegistry registry;
    private final CpuMetrics cpuMetrics;
    private final MemoryMetrics memoryMetrics;
    private final IOMetrics ioMetrics;
    private final NetworkMetrics networkMetrics;

    /**
     * Constructs a new {@code SystemMetrics} instance.
     * This constructor initializes all underlying metric category collectors.
     */
    public SystemMetrics() {
        this.systemInfo = new SystemInfo();
        this.registry = new StarRocksMetricRegistry();
        this.cpuMetrics = new CpuMetrics(systemInfo.getHardware().getProcessor());
        this.memoryMetrics = new MemoryMetrics(systemInfo.getHardware().getMemory());
        this.ioMetrics = new IOMetrics(systemInfo.getOperatingSystem());
        this.networkMetrics = new NetworkMetrics(systemInfo.getHardware(), systemInfo.getOperatingSystem());
    }

    public void init() {
        cpuMetrics.init(registry);
        memoryMetrics.init(registry);
        ioMetrics.init(registry);
        networkMetrics.init(registry);
    }

    /**
     * Atomically updates all system metrics by fetching the latest values from the system.
     */
    public synchronized void update() {
        cpuMetrics.update();
        memoryMetrics.update();
        ioMetrics.update();
        networkMetrics.update();
    }

    public void collect(MetricVisitor visitor) {
        for (Metric<?> metric : registry.getMetrics()) {
            visitor.visit(metric);
        }
    }

    @VisibleForTesting
    StarRocksMetricRegistry getRegistry() {
        return registry;
    }

    /**
     * Collects and reports CPU utilization metrics, broken down by various states.
     *
     * <p><b>Usage Scenario:</b>
     * This class is used to monitor the CPU load on the system, providing a detailed breakdown of time spent
     * in different modes such as user, system, idle, and I/O wait.
     *
     * <p><b>Implementation Mechanism:</b>
     * It leverages {@code oshi}'s {@link CentralProcessor} to get cumulative CPU load ticks for each state
     * and exposes them as individual gauge metrics.
     */
    private static class CpuMetrics {
        /** The {@code oshi} object for accessing CPU information. */
        final CentralProcessor centralProcessor;
        /** Gauge metric for CPU time spent in user mode. */
        final GaugeMetricImpl<Long> user;
        /** Gauge metric for CPU time spent in user mode with low priority (nice). */
        final GaugeMetricImpl<Long> nice;
        /** Gauge metric for CPU time spent in system/kernel mode. */
        final GaugeMetricImpl<Long> system;
        /** Gauge metric for CPU time spent idle. */
        final GaugeMetricImpl<Long> idle;
        /** Gauge metric for CPU time spent waiting for I/O to complete. */
        final GaugeMetricImpl<Long> iowait;
        /** Gauge metric for CPU time spent servicing hardware interrupts. */
        final GaugeMetricImpl<Long> irq;
        /** Gauge metric for CPU time spent servicing software interrupts. */
        final GaugeMetricImpl<Long> softirq;
        /** Gauge metric for involuntary wait time, representing time stolen by a virtual machine hypervisor. */
        final GaugeMetricImpl<Long> steal;

        /**
         * Constructs a {@code CpuMetrics} instance and initializes all CPU-related gauge metrics.
         *
         * @param centralProcessor An {@code oshi} {@link CentralProcessor} instance to query CPU stats from.
         */
        public CpuMetrics(CentralProcessor centralProcessor) {
            this.centralProcessor = centralProcessor;
            this.user = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "User CPU time");
            this.user.addLabel(new MetricLabel("mode", "user"));
            this.nice = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "Nice CPU time");
            this.nice.addLabel(new MetricLabel("mode", "nice"));
            this.system = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "System CPU time");
            this.system.addLabel(new MetricLabel("mode", "system"));
            this.idle = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "Idle CPU time");
            this.idle.addLabel(new MetricLabel("mode", "idle"));
            this.iowait = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "IOWait CPU time");
            this.iowait.addLabel(new MetricLabel("mode", "iowait"));
            this.irq = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "IRQ CPU time");
            this.irq.addLabel(new MetricLabel("mode", "irq"));
            this.softirq = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "Softirq CPU time");
            this.softirq.addLabel(new MetricLabel("mode", "softirq"));
            this.steal = new GaugeMetricImpl<>(
                    "cpu", Metric.MetricUnit.NOUNIT, "Steal CPU time");
            this.steal.addLabel(new MetricLabel("mode", "steal"));
        }

        /**
         * Registers all CPU metrics with the given registry.
         *
         * @param registry The metric registry to which the metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            registry.addMetric(user);
            registry.addMetric(nice);
            registry.addMetric(system);
            registry.addMetric(idle);
            registry.addMetric(iowait);
            registry.addMetric(irq);
            registry.addMetric(softirq);
            registry.addMetric(steal);
            LOG.info("init cpu metrics");
        }

        /**
         * Fetches the latest CPU tick counters from the system and updates the corresponding gauge metrics.
         */
        public void update() {
            try {
                long[] ticks = centralProcessor.getSystemCpuLoadTicks();
                user.setValue(getTicks(ticks, CentralProcessor.TickType.USER));
                nice.setValue(getTicks(ticks, CentralProcessor.TickType.NICE));
                system.setValue(getTicks(ticks, CentralProcessor.TickType.SYSTEM));
                idle.setValue(getTicks(ticks, CentralProcessor.TickType.IDLE));
                iowait.setValue(getTicks(ticks, CentralProcessor.TickType.IOWAIT));
                irq.setValue(getTicks(ticks, CentralProcessor.TickType.IRQ));
                softirq.setValue(getTicks(ticks, CentralProcessor.TickType.SOFTIRQ));
                steal.setValue(getTicks(ticks, CentralProcessor.TickType.STEAL));
            } catch (Exception e) {
                LOG.warn("failed to update cpu metrics", e);
            }
        }

        /**
         * Safely extracts a specific tick value from the ticks array based on its type.
         *
         * @param ticks The array of CPU tick counters, where each index corresponds to a {@link CentralProcessor.TickType}.
         * @param tickType The type of tick to retrieve (e.g., USER, SYSTEM).
         * @return The corresponding tick value, or 0L if the index is out of bounds for the given array.
         */
        private long getTicks(long[] ticks, CentralProcessor.TickType tickType) {
            int index = tickType.getIndex();
            return index < ticks.length ? ticks[index] : 0L;
        }
    }

    /**
     * Collects and reports system memory and swap space usage.
     *
     * <p><b>Usage Scenario:</b>
     * This class is used to monitor the physical and virtual memory consumption of the host system,
     * which is crucial for capacity planning and performance tuning.
     *
     * <p><b>Implementation Mechanism:</b>
     * It uses {@code oshi}'s {@link GlobalMemory} to query total, available, and swap memory statistics
     * and reports them as gauge metrics.
     */
    private static class MemoryMetrics {
        /** The {@code oshi} object for accessing global memory information. */
        final GlobalMemory globalMemory;
        /** Gauge metric for the total amount of physical memory in bytes. */
        final GaugeMetricImpl<Long> total;
        /** Gauge metric for the amount of used physical memory in bytes. */
        final GaugeMetricImpl<Long> used;
        /** Gauge metric for the total amount of swap space in bytes. */
        final GaugeMetricImpl<Long> swapTotal;
        /** Gauge metric for the amount of used swap space in bytes. */
        final GaugeMetricImpl<Long> swapUsed;

        /**
         * Constructs a {@code MemoryMetrics} instance and initializes memory-related gauge metrics.
         *
         * @param globalMemory An {@code oshi} {@link GlobalMemory} instance for querying memory information.
         */
        public MemoryMetrics(GlobalMemory globalMemory) {
            this.globalMemory = globalMemory;
            this.total = new GaugeMetricImpl<>(
                    "memory", Metric.MetricUnit.NOUNIT, "Total memory");
            this.total.addLabel(new MetricLabel("name", "total"));
            this.used = new GaugeMetricImpl<>(
                    "memory", Metric.MetricUnit.NOUNIT, "Used memory");
            this.used.addLabel(new MetricLabel("name", "used"));
            this.swapTotal = new GaugeMetricImpl<>(
                    "memory", Metric.MetricUnit.NOUNIT, "Total swap memory");
            this.swapTotal.addLabel(new MetricLabel("name", "swap_total"));
            this.swapUsed = new GaugeMetricImpl<>(
                    "memory", Metric.MetricUnit.NOUNIT, "Used swap memory");
            this.swapUsed.addLabel(new MetricLabel("name", "swap_used"));
        }

        /**
         * Registers all memory-related metrics with the given registry.
         *
         * @param registry The metric registry to which the metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            registry.addMetric(total);
            registry.addMetric(used);
            registry.addMetric(swapTotal);
            registry.addMetric(swapUsed);
            LOG.info("init memory metrics");
        }

        /**
         * Fetches the latest memory usage statistics from the system and updates the gauge metrics.
         */
        public void update() {
            try {
                long totalMem = globalMemory.getTotal();
                long availableMem = globalMemory.getAvailable();
                long usedMem = totalMem - availableMem;

                total.setValue(totalMem);
                used.setValue(usedMem);

                VirtualMemory vm = globalMemory.getVirtualMemory();
                if (vm != null) {
                    long swapTotalBytes = vm.getSwapTotal();
                    long swapUsedBytes = vm.getSwapUsed();

                    swapTotal.setValue(swapTotalBytes);
                    swapUsed.setValue(swapUsedBytes);
                }
            } catch (Exception e) {
                LOG.warn("failed to update memory metrics", e);
            }
        }
    }

    /**
     * Represents and manages I/O and capacity metrics for a single disk device.
     *
     * <p><b>Usage Scenario:</b>
     * This class is used internally by {@link IOMetrics} to track capacity and I/O statistics for a specific
     * monitored disk, such as the one hosting metadata or log directories.
     *
     * <p><b>Implementation Mechanism:</b>
     * It holds a set of gauge metrics for a given disk, identified by its device name.
     * Capacity is read via JDK's {@link FileStore}, while detailed I/O statistics (like read/write counts and times)
     * are supplied externally from a source like {@code /proc/diskstats}.
     * 
     * @note This implementation is Linux-specific due to its reliance on the {@code /proc/diskstats} file.
     */
    private static class DiskMetrics {
        /** The device name of the disk (e.g., "sda"). */
        final String name;
        /** The JDK's {@link FileStore} object associated with the disk's mount point. */
        final FileStore fileStore;
        /** Gauge for the total capacity of the disk in bytes. */
        final GaugeMetricImpl<Long> totalCapacity;
        /** Gauge for the used capacity of the disk in bytes. */
        final GaugeMetricImpl<Long> usedCapacity;
        /** Gauge for the total number of reads completed successfully. */
        final GaugeMetricImpl<Long> readsCompleted;
        /** Gauge for the total number of bytes read from the disk. */
        final GaugeMetricImpl<Long> bytesRead;
        /** Gauge for the cumulative time spent reading from the disk, in milliseconds. */
        final GaugeMetricImpl<Long> readTimeMs;
        /** Gauge for the total number of writes completed successfully. */
        final GaugeMetricImpl<Long> writesCompleted;
        /** Gauge for the cumulative time spent writing to the disk, in milliseconds. */
        final GaugeMetricImpl<Long> writeTimeMs;
        /** Gauge for the total number of bytes written to the disk. */
        final GaugeMetricImpl<Long> bytesWritten;
        /** Gauge for the cumulative time spent doing I/Os, in milliseconds. */
        final GaugeMetricImpl<Long> ioTimeMs;
        /** Gauge for the weighted cumulative time spent doing I/Os, in milliseconds. */
        final GaugeMetricImpl<Long> ioTimeWeightedMs;
        
        /**
         * Constructs a {@code DiskMetrics} instance for a specific disk device.
         *
         * @param name The device name (e.g., "sda").
         * @param fileStore The {@link FileStore} associated with a mount point on this device, used to get capacity info.
         */
        public DiskMetrics(String name, FileStore fileStore) {
            this.name = name;
            this.fileStore = fileStore;
            this.totalCapacity = new GaugeMetricImpl<>(
                    "disk_total_capacity", Metric.MetricUnit.BYTES, "Total capacity of the disk");
            this.totalCapacity.addLabel(new MetricLabel("device", name));
            this.usedCapacity = new GaugeMetricImpl<>(
                    "disk_used_capacity", Metric.MetricUnit.BYTES, "Used capacity of the disk");
            this.usedCapacity.addLabel(new MetricLabel("device", name));
            this.readsCompleted = new GaugeMetricImpl<>(
                    "disk_reads_completed", Metric.MetricUnit.NOUNIT, "Reads completed");
            this.readsCompleted.addLabel(new MetricLabel("device", name));
            this.bytesRead = new GaugeMetricImpl<>(
                    "disk_bytes_read", Metric.MetricUnit.BYTES, "Bytes read");
            this.bytesRead.addLabel(new MetricLabel("device", name));
            this.readTimeMs = new GaugeMetricImpl<>(
                    "disk_read_time_ms", Metric.MetricUnit.MILLISECONDS, "Read time in milliseconds");
            this.readTimeMs.addLabel(new MetricLabel("device", name));
            this.writesCompleted = new GaugeMetricImpl<>(
                    "disk_writes_completed", Metric.MetricUnit.NOUNIT, "Writes completed");
            this.writesCompleted.addLabel(new MetricLabel("device", name));
            this.bytesWritten = new GaugeMetricImpl<>(
                    "disk_bytes_written", Metric.MetricUnit.BYTES, "Bytes written");
            this.bytesWritten.addLabel(new MetricLabel("device", name));
            this.writeTimeMs = new GaugeMetricImpl<>(
                    "disk_write_time_ms", Metric.MetricUnit.MILLISECONDS, "Write time in milliseconds");
            this.writeTimeMs.addLabel(new MetricLabel("device", name));
            this.ioTimeMs = new GaugeMetricImpl<>(
                    "disk_io_time_ms", Metric.MetricUnit.MILLISECONDS, "IO time in milliseconds");
            this.ioTimeMs.addLabel(new MetricLabel("device", name));
            this.ioTimeWeightedMs = new GaugeMetricImpl<>(
                    "disk_io_time_weighted_ms", Metric.MetricUnit.MILLISECONDS, "Weighted IO time in milliseconds");
            this.ioTimeWeightedMs.addLabel(new MetricLabel("device", name));
        }

        /**
         * Registers all metrics for this disk with the specified registry.
         *
         * @param registry The metric registry to which the metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            registry.addMetric(totalCapacity);
            registry.addMetric(usedCapacity);
            registry.addMetric(readsCompleted);
            registry.addMetric(bytesRead);
            registry.addMetric(readTimeMs);
            registry.addMetric(writesCompleted);
            registry.addMetric(bytesWritten);
            registry.addMetric(writeTimeMs);
            registry.addMetric(ioTimeMs);
            registry.addMetric(ioTimeWeightedMs);
            LOG.info("init disk metrics for {}", name);
        }

        /**
         * Updates all disk I/O and capacity metrics with the latest values.
         *
         * @param readsCompleted The total number of reads completed.
         * @param bytesRead The total number of bytes read.
         * @param readTimeMs The cumulative time spent reading, in milliseconds.
         * @param writesCompleted The total number of writes completed.
         * @param bytesWritten The total number of bytes written.
         * @param writeTimeMs The cumulative time spent writing, in milliseconds.
         * @param ioTimeMs The cumulative time spent doing I/Os, in milliseconds.
         * @param ioTimeWeightedMs The weighted cumulative time spent doing I/Os, in milliseconds.
         */
        public void updateStat(long readsCompleted, long bytesRead, long readTimeMs, long writesCompleted,
                long bytesWritten, long writeTimeMs, long ioTimeMs, long ioTimeWeightedMs) {
            try {
                long total = fileStore.getTotalSpace();
                long used = total - fileStore.getUnallocatedSpace();
                totalCapacity.setValue(total);
                usedCapacity.setValue(used);
            } catch (Exception e) {
                LOG.warn("failed to update capacity for disk: {}", name, e);
            }
            this.readsCompleted.setValue(readsCompleted);
            this.bytesRead.setValue(bytesRead);
            this.readTimeMs.setValue(readTimeMs);
            this.writesCompleted.setValue(writesCompleted);
            this.bytesWritten.setValue(bytesWritten);
            this.writeTimeMs.setValue(writeTimeMs);
            this.ioTimeMs.setValue(ioTimeMs);
            this.ioTimeWeightedMs.setValue(ioTimeWeightedMs);
        }
    }

    /**
     * Collects and reports disk I/O and capacity metrics for file systems relevant to StarRocks.
     *
     * <p><b>Usage Scenario:</b>
     * This class is used to monitor the disk I/O activity and storage usage for directories critical to StarRocks operations,
     * such as the metadata directory ({@code Config.meta_dir}) and the system log directory ({@code Config.sys_log_dir}).
     *
     * <p><b>Implementation Mechanism:</b>
     * It automatically identifies the underlying disk devices for the configured paths. It then parses {@code /proc/diskstats}
     * to obtain detailed, low-level I/O statistics for these specific devices. Disk capacity is determined using JDK's 
     * {@link FileStore}.
     *
     * @note This implementation is Linux-specific due to its reliance on the {@code /proc/diskstats} file.
     */
    private static class IOMetrics {

        // Paths to monitor
        private static final List<String> MONITOR_PATHS = Arrays.asList(Config.meta_dir, Config.sys_log_dir);
        
        private final OperatingSystem operatingSystem;
        // disk name -> disk metrics
        private final Map<String, DiskMetrics> metricsForEachDisk;

        /**
         * Constructs an {@code IOMetrics} instance.
         *
         * @param operatingSystem An {@code oshi} {@link OperatingSystem} instance, used for filesystem interactions.
         */
        public IOMetrics(OperatingSystem operatingSystem) {
            this.operatingSystem = operatingSystem;
            this.metricsForEachDisk = new HashMap<>();
        }

        /**
         * Discovers the disks associated with monitored paths and initializes metrics for them.
         *
         * <p><b>Implementation Details:</b>
         * This method first calls {@code getDiskNameAndMounts} to find the physical devices corresponding to the
         * paths defined in {@code MONITOR_PATHS} (e.g., {@code Config.meta_dir}, {@code Config.sys_log_dir}).
         * It then creates and initializes a {@link DiskMetrics} object for each unique device discovered.
         *
         * @param registry The metric registry to which the disk metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            try {
                List<Pair<String, String>> diskNameAndMounts = getDiskNameAndMounts(MONITOR_PATHS);
                for (Pair<String, String> pair : diskNameAndMounts) {
                    String name = pair.first;
                    String mount = pair.second;
                    FileStore fileStore = Files.getFileStore(Paths.get(mount));
                    DiskMetrics metrics = new DiskMetrics(name, fileStore);
                    metricsForEachDisk.put(name, metrics);
                    metrics.init(registry);
                }
                LOG.info("init io metrics");
            } catch (Exception e) {
                LOG.warn("failed to init disk metrics", e);
            }
        }

        /**
         * Updates disk I/O metrics by reading and parsing the {@code /proc/diskstats} file.
         *
         * <p><b>Implementation Details:</b>
         * This method reads {@code /proc/diskstats} line by line. Each line contains performance statistics for a
         * specific block device. If the device name matches one of the monitored disks, the line is parsed, and the
         * corresponding {@link DiskMetrics} object is updated with the new values.
         */
        public void update() {
            String procFile = "/proc/diskstats";
            if (Files.notExists(Paths.get(procFile))) {
                LOG.debug("can't find {}", procFile);
                return;
            }
            try (FileReader fileReader = new FileReader(procFile);
                    BufferedReader bufferedReader = new BufferedReader(fileReader)) {
                String line;
                while ((line = bufferedReader.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) {
                        continue;
                    }
                    // Format (first 14 fields):
                    // 0:major 1:minor 2:name 3:reads_completed 4:reads_merged 5:sectors_read 6:time_reading_ms
                    // 7:writes_completed 8:writes_merged 9:sectors_written 10:time_writing_ms
                    // 11:ios_in_progress 12:io_time_ms 13:io_time_weighted_ms
                    String[] parts = line.split("\\s+");
                    if (parts.length < 14) {
                        continue;
                    }

                    String device = parts[2];
                    DiskMetrics metrics = metricsForEachDisk.get(device);
                    if (metrics == null) {
                        continue;
                    }

                    try {
                        long readsCompleted = Long.parseLong(parts[3]);
                        long sectorsRead = Long.parseLong(parts[5]);
                        long readTimeMs = Long.parseLong(parts[6]);
                        long writesCompleted = Long.parseLong(parts[7]);
                        long sectorsWritten = Long.parseLong(parts[9]);
                        long writeTimeMs = Long.parseLong(parts[10]);
                        long ioTimeMs = Long.parseLong(parts[12]);
                        long ioTimeWeightedMs = Long.parseLong(parts[13]);

                        // A sector is traditionally 512 bytes.
                        long bytesRead = sectorsRead << 9; // sectors * 512
                        long bytesWritten = sectorsWritten << 9; // sectors * 512

                        metrics.updateStat(readsCompleted, bytesRead, readTimeMs,
                                writesCompleted, bytesWritten, writeTimeMs,
                                ioTimeMs, ioTimeWeightedMs);
                    } catch (Exception ie) {
                        // skip malformed line
                        LOG.warn("failed to parse /proc/diskstats line for device {}: {}", device, line, ie);
                    }
                }
            } catch (Exception e) {
                LOG.warn("failed to read disk stat", e);
            }
        }

        /**
         * Identifies the unique disk devices and their corresponding mount points for a list of file paths.
         *
         * <p><b>Implementation Details:</b>
         * For each given path, this method finds the most specific oshi {@link OSFileStore} by searching for the one
         * with the longest matching mount point prefix against the path. This approach correctly handles nested mounts
         * and symbolic links. After identifying the file store, it extracts the base device name (e.g., "sda")
         * from the volume path.
         *
         * @param monitorPaths A list of absolute or relative file paths to check.
         * @return A list of pairs, where each pair contains a disk device name and its associated mount point.
         */
        private List<Pair<String, String>> getDiskNameAndMounts(List<String> monitorPaths) {
            List<OSFileStore> allFileStores = operatingSystem.getFileSystem().getFileStores();
            Set<OSFileStore> monitorFileStores = Collections.newSetFromMap(new IdentityHashMap<>());
            for (String path : monitorPaths) {
                try {
                    String realPath = Paths.get(path).toAbsolutePath().normalize().toString();
                    OSFileStore matchedFileStore = null;
                    for (OSFileStore fileStore : allFileStores) {
                        String mount = fileStore.getMount();
                        if (realPath.startsWith(mount) &&
                                (matchedFileStore == null
                                        || mount.length() > matchedFileStore.getMount().length())) {
                            matchedFileStore = fileStore;
                        }
                    }
                    if (matchedFileStore != null) {
                        monitorFileStores.add(matchedFileStore);
                        LOG.info("monitor IO for path: {}, disk: {}, mount: {}",
                                path, matchedFileStore.getVolume(), matchedFileStore.getMount());
                    }
                } catch (Exception e) {
                    LOG.warn("failed to get disk information for path: {}", path, e);
                }
            }

            List<Pair<String, String>> diskNameAndMounts = new ArrayList<>();
            for (OSFileStore fileStore : monitorFileStores) {
                String name = Paths.get(fileStore.getVolume()).getFileName().toString();
                String mount = fileStore.getMount();
                diskNameAndMounts.add(Pair.create(name, mount));
            }
            return diskNameAndMounts;
        }
    }

    /**
     * Collects and reports TCP connection statistics derived from the system's protocol stats.
     *
     * <p><b>Usage Scenario:</b>
     * This class provides insights into the state and performance of the TCP network stack, such as connection rates,
     * failures, resets, and segment traffic. It is useful for diagnosing network-related issues.
     *
     * <p><b>Implementation Mechanism:</b>
     * It uses {@code oshi}'s {@link InternetProtocolStats} to retrieve TCP statistics for both IPv4 and IPv6
     * (if enabled). The values from both protocol versions are aggregated and exposed as a set of gauge metrics.
     */
    private static class TcpMetrics {
        /** The {@code oshi} object for accessing operating system-level information. */
        final OperatingSystem operatingSystem;
        /** The number of times TCP connections have made a direct transition from the CLOSED state to the SYN-SENT state. */
        final GaugeMetricImpl<Long> activeOpens;
        /** The number of times TCP connections have made a direct transition from the LISTEN state to the SYN-RCVD state. */
        final GaugeMetricImpl<Long> passiveOpens;
        /** The number of failed connection attempts. */
        final GaugeMetricImpl<Long> attemptsFails;
        /** The number of established connections that were reset. */
        final GaugeMetricImpl<Long> estabResets;
        /** The number of TCP connections currently in the ESTABLISHED or CLOSE-WAIT state. */
        final GaugeMetricImpl<Long> currEstab;
        /** The total number of TCP segments received. */
        final GaugeMetricImpl<Long> tcpInSegs;
        /** The total number of TCP segments sent. */
        final GaugeMetricImpl<Long> tcpOutSegs;
        /** The total number of TCP segments retransmitted. */
        final GaugeMetricImpl<Long> tcpRetransSegs;
        /** The number of TCP segments received in error (e.g., bad checksums). */
        final GaugeMetricImpl<Long> tcpInErrs;
        /** The number of TCP segments sent containing the RST flag. */
        final GaugeMetricImpl<Long> tcpOutRsts;
    
        /**
         * Constructs a {@code TcpMetrics} instance.
         *
         * @param operatingSystem An {@code oshi} {@link OperatingSystem} instance for querying protocol stats.
         */
        public TcpMetrics(OperatingSystem operatingSystem) {
            this.operatingSystem = operatingSystem;
            this.activeOpens = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of active opens");
            this.activeOpens.addLabel(new MetricLabel("name", "active_opens"));
            this.passiveOpens = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of passive opens");
            this.passiveOpens.addLabel(new MetricLabel("name", "passive_opens"));
            this.attemptsFails = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of attempts fails");
            this.attemptsFails.addLabel(new MetricLabel("name", "attempts_fails"));
            this.estabResets = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of estab resets");
            this.estabResets.addLabel(new MetricLabel("name", "estab_resets"));
            this.currEstab = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of current establishes");
            this.currEstab.addLabel(new MetricLabel("name", "curr_estab"));
            this.tcpInSegs = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of all TCP packets received");
            this.tcpInSegs.addLabel(new MetricLabel("name", "tcp_in_segs"));
            this.tcpOutSegs = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of all TCP packets send with RST");
            this.tcpOutSegs.addLabel(new MetricLabel("name", "tcp_out_segs"));
            this.tcpRetransSegs = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "All TCP packets retransmitted");
            this.tcpRetransSegs.addLabel(new MetricLabel("name", "tcp_retrans_segs"));
            this.tcpInErrs = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of all problematic TCP packets received");
            this.tcpInErrs.addLabel(new MetricLabel("name", "tcp_in_errs"));
            this.tcpOutRsts = new GaugeMetricImpl<>(
                    "snmp", Metric.MetricUnit.NOUNIT, "The number of all TCP packets send with RST");
            this.tcpOutRsts.addLabel(new MetricLabel("name", "tcp_out_rsts"));
        }

        /**
         * Registers all TCP-related metrics with the specified registry.
         *
         * @param registry The metric registry to which the metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            registry.addMetric(activeOpens);
            registry.addMetric(passiveOpens);
            registry.addMetric(attemptsFails);
            registry.addMetric(estabResets);
            registry.addMetric(currEstab);
            registry.addMetric(tcpInSegs);
            registry.addMetric(tcpOutSegs);
            registry.addMetric(tcpRetransSegs);
            registry.addMetric(tcpInErrs);
            registry.addMetric(tcpOutRsts);
            LOG.info("init tcp metrics");
        }

        /**
         * Fetches the latest TCP statistics from the system and updates the gauge metrics.
         */
        public void update() {
            try {
                InternetProtocolStats ipStats = operatingSystem.getInternetProtocolStats();
                InternetProtocolStats.TcpStats tcpStatsV4 = ipStats.getTCPv4Stats();
                InternetProtocolStats.TcpStats tcpStatsV6 = FrontendOptions.isBindIPV6() ? ipStats.getTCPv6Stats() : null;

                updateStat(activeOpens, InternetProtocolStats.TcpStats::getConnectionsActive, tcpStatsV4, tcpStatsV6);
                updateStat(passiveOpens, InternetProtocolStats.TcpStats::getConnectionsPassive, tcpStatsV4, tcpStatsV6);
                updateStat(attemptsFails, InternetProtocolStats.TcpStats::getConnectionFailures, tcpStatsV4, tcpStatsV6);
                updateStat(estabResets, InternetProtocolStats.TcpStats::getConnectionsReset, tcpStatsV4, tcpStatsV6);
                updateStat(currEstab, InternetProtocolStats.TcpStats::getConnectionsEstablished, tcpStatsV4, tcpStatsV6);
                updateStat(tcpInSegs, InternetProtocolStats.TcpStats::getSegmentsReceived, tcpStatsV4, tcpStatsV6);
                updateStat(tcpOutSegs, InternetProtocolStats.TcpStats::getSegmentsSent, tcpStatsV4, tcpStatsV6);
                updateStat(tcpRetransSegs, InternetProtocolStats.TcpStats::getSegmentsRetransmitted, tcpStatsV4, tcpStatsV6);
                updateStat(tcpInErrs, InternetProtocolStats.TcpStats::getInErrors, tcpStatsV4, tcpStatsV6);
                updateStat(tcpOutRsts, InternetProtocolStats.TcpStats::getOutResets, tcpStatsV4, tcpStatsV6);
            } catch (Exception e) {
                LOG.warn("failed to update tcp metrics", e);
            }
        }

        /**
         * Helper method to update a gauge metric by summing a specific statistic from both IPv4 and IPv6 TCP stats.
         *
         * @param metric The gauge metric to update.
         * @param statSupplier A function that extracts a specific long value from a 
         *      {@code oshi}'s {@link InternetProtocolStats.TcpStats} object.
         * @param tcpStatsV4 The TCP statistics for IPv4. Can be null.
         * @param tcpStatsV6 The TCP statistics for IPv6. Can be null.
         */
        private void updateStat(GaugeMetricImpl<Long> metric, Function<InternetProtocolStats.TcpStats, Long> statSupplier,
                               InternetProtocolStats.TcpStats tcpStatsV4, InternetProtocolStats.TcpStats tcpStatsV6) {
            long value = (tcpStatsV4 != null ? statSupplier.apply(tcpStatsV4) : 0L);
            value += (tcpStatsV6 != null ? statSupplier.apply(tcpStatsV6) : 0L);
            metric.setValue(value);
        }
    }

    /**
     * Collects and reports I/O metrics for a single network interface.
     *
     * <p><b>Usage Scenario:</b>
     * This class is used internally by {@link NetworkMetrics} to track the traffic and error rates for each
     * active network interface on the system.
     *
     * <p><b>Implementation Mechanism:</b>
     * It holds a set of gauge metrics for a given network interface (e.g., "eth0"). The {@link #update()} method
     * calls {@link NetworkIF#updateAttributes()} to refresh the underlying statistics before updating the metrics.
     */
    private static class NetIfMetrics {
        /** The {@code oshi} object representing the network interface. */
        final NetworkIF netIf;
        /** Gauge for the total number of bytes received on the interface. */
        final GaugeMetricImpl<Long> receiveBytes;
        /** Gauge for the total number of packets received on the interface. */
        final GaugeMetricImpl<Long> receivePackets;
        /** Gauge for the number of receive errors on the interface. */
        final GaugeMetricImpl<Long> receiveErrors;
        /** Gauge for the number of incoming packets that were dropped. */
        final GaugeMetricImpl<Long> receiveDropped;
        /** Gauge for the total number of bytes sent from the interface. */
        final GaugeMetricImpl<Long> sendBytes;
        /** Gauge for the total number of packets sent from the interface. */
        final GaugeMetricImpl<Long> sendPackets;
        /** Gauge for the number of send errors on the interface. */
        final GaugeMetricImpl<Long> sendErrors;

        /**
         * Constructs a {@code NetIfMetrics} instance for a specific network interface.
         *
         * @param netIf The {@code oshi} {@link NetworkIF} object to monitor.
         */
        public NetIfMetrics(NetworkIF netIf) {
            this.netIf = netIf;
            this.receiveBytes = new GaugeMetricImpl<>(
                    "network_receive_bytes", Metric.MetricUnit.BYTES, "Bytes received");
            this.receiveBytes.addLabel(new MetricLabel("device", netIf.getName()));
            this.receivePackets = new GaugeMetricImpl<>(
                    "network_receive_packets", Metric.MetricUnit.NOUNIT, "Packets received");
            this.receivePackets.addLabel(new MetricLabel("device", netIf.getName()));
            this.receiveErrors = new GaugeMetricImpl<>(
                    "network_receive_errors", Metric.MetricUnit.NOUNIT, "Receive errors");
            this.receiveErrors.addLabel(new MetricLabel("device", netIf.getName()));
            this.receiveDropped = new GaugeMetricImpl<>(
                    "network_receive_dropped", Metric.MetricUnit.NOUNIT, "Receive dropped");
            this.receiveDropped.addLabel(new MetricLabel("device", netIf.getName()));
            this.sendBytes = new GaugeMetricImpl<>(
                    "network_send_bytes", Metric.MetricUnit.BYTES, "Bytes sent");
            this.sendBytes.addLabel(new MetricLabel("device", netIf.getName()));
            this.sendPackets = new GaugeMetricImpl<>(
                    "network_send_packets", Metric.MetricUnit.NOUNIT, "Packets sent");
            this.sendPackets.addLabel(new MetricLabel("device", netIf.getName()));
            this.sendErrors = new GaugeMetricImpl<>(
                    "network_send_errors", Metric.MetricUnit.NOUNIT, "Send errors");
            this.sendErrors.addLabel(new MetricLabel("device", netIf.getName()));
        }

        /**
         * Registers all metrics for this network interface with the specified registry.
         *
         * @param registry The metric registry to which the metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            registry.addMetric(receiveBytes);
            registry.addMetric(receivePackets);
            registry.addMetric(receiveErrors);
            registry.addMetric(receiveDropped);
            registry.addMetric(sendBytes);
            registry.addMetric(sendPackets);
            registry.addMetric(sendErrors);
            LOG.info("init metrics for network interface: {}", netIf.getName());
        }

        /**
         * Fetches the latest I/O statistics for the network interface and updates the gauge metrics.
         */
        public void update() {
            try {
                netIf.updateAttributes();
                receiveBytes.setValue(netIf.getBytesRecv());
                receivePackets.setValue(netIf.getPacketsRecv());
                receiveErrors.setValue(netIf.getInErrors());
                receiveDropped.setValue(netIf.getInDrops());
                sendBytes.setValue(netIf.getBytesSent());
                sendPackets.setValue(netIf.getPacketsSent());
                sendErrors.setValue(netIf.getOutErrors());
            } catch (Exception e) {
                LOG.warn("failed to update metrics for network interface {}", netIf.getName(), e);
            }
        }
    }

    /**
     * Collects and reports comprehensive network metrics, including per-interface I/O and overall TCP statistics.
     *
     * <p><b>Usage Scenario:</b>
     * This class provides a complete view of the system's network activity. It is used to monitor network traffic,
     * error rates, and TCP connection behavior to ensure network health and performance.
     *
     * <p><b>Implementation Mechanism:</b>
     * It discovers all active network interfaces (those with an IPv4 or IPv6 address) and creates a {@link NetIfMetrics}
     * instance for each one. It also instantiates a {@link TcpMetrics} object to gather TCP protocol statistics.
     * The {@code update} method then triggers updates for all underlying collectors.
     */
    private static class NetworkMetrics {

        final HardwareAbstractionLayer hardware;
        final List<NetIfMetrics> metricsForEachNetIf;
        final TcpMetrics tcpMetrics;

        /**
         * Constructs a {@code NetworkMetrics} instance.
         *
         * @param hardware The {@code oshi} {@link HardwareAbstractionLayer} for discovering network interfaces.
         * @param operatingSystem The {@code oshi} {@link OperatingSystem} for querying TCP statistics.
         */
        public NetworkMetrics(HardwareAbstractionLayer hardware, OperatingSystem operatingSystem) {
            this.hardware = hardware;
            this.metricsForEachNetIf = new ArrayList<>();
            this.tcpMetrics = new TcpMetrics(operatingSystem);
        }

        /**
         * Discovers active network interfaces and initializes metrics for them and for the TCP protocol.
         *
         * @param registry The metric registry to which all network metrics will be added.
         */
        public void init(StarRocksMetricRegistry registry) {
            boolean isBindIPV6 = FrontendOptions.isBindIPV6();
            List<NetworkIF> interfaces = hardware.getNetworkIFs(false).stream()
                    .filter(netIf -> netIf.getIPv4addr().length > 0 || (isBindIPV6 && netIf.getIPv6addr().length > 0))
                    .toList();
            for (NetworkIF netIf : interfaces) {
                NetIfMetrics metrics = new NetIfMetrics(netIf);
                metricsForEachNetIf.add(metrics);
                metrics.init(registry);
            }
            tcpMetrics.init(registry);
            LOG.info("init network metrics");
        }

        /**
         * Updates all network-related metrics, including per-interface I/O and TCP statistics.
         */
        public void update() {
            metricsForEachNetIf.forEach(NetIfMetrics::update);
            tcpMetrics.update();
        }
    }
}
