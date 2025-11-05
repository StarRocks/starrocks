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
import com.starrocks.service.FrontendOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.NetworkIF;
import oshi.hardware.VirtualMemory;
import oshi.software.os.FileSystem;
import oshi.software.os.InternetProtocolStats;
import oshi.software.os.OSFileStore;
import oshi.software.os.OperatingSystem;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SystemMetricsTest {

    // Test-scoped mocks
    private CentralProcessor cpu;
    private GlobalMemory memory;
    private HardwareAbstractionLayer hal;
    private OperatingSystem os;
    private FileSystem fs;
    private OSFileStore storeRoot;
    private NetworkIF netIf;
    private InternetProtocolStats ipStats;
    private InternetProtocolStats.TcpStats v4;
    private InternetProtocolStats.TcpStats v6;
    private VirtualMemory vm;
    private MockedConstruction<SystemInfo> systemInfoConstruction;
    private SystemMetrics sm;

    @BeforeEach
    public void setUp() throws Exception {
        cpu = mock(CentralProcessor.class);
        memory = mock(GlobalMemory.class);
        hal = mock(HardwareAbstractionLayer.class);
        os = mock(OperatingSystem.class);
        fs = mock(FileSystem.class);
        storeRoot = mock(OSFileStore.class);
        netIf = mock(NetworkIF.class);
        ipStats = mock(InternetProtocolStats.class);
        v4 = mock(InternetProtocolStats.TcpStats.class);
        v6 = mock(InternetProtocolStats.TcpStats.class);
        vm = mock(VirtualMemory.class);
        // Bind new SystemInfo() to mocked HAL/OS; wire HAL to CPU/Memory
        systemInfoConstruction = Mockito.mockConstruction(SystemInfo.class, (mockSystemInfo, context) -> {
            when(mockSystemInfo.getHardware()).thenReturn(hal);
            when(mockSystemInfo.getOperatingSystem()).thenReturn(os);
        });
        when(hal.getProcessor()).thenReturn(cpu);
        when(hal.getMemory()).thenReturn(memory);
        sm = new SystemMetrics();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (systemInfoConstruction != null) {
            systemInfoConstruction.close();
        }
    }

    @Test
    public void testInit() {
        // NetIF discovery stubs
        when(netIf.getName()).thenReturn("eth0");
        when(netIf.getIPv4addr()).thenReturn(new String[] {"127.0.0.1"});
        when(hal.getNetworkIFs(false)).thenReturn(List.of(netIf));

        // Filesystem discovery stubs
        Config.meta_dir = "/var/starrocks/meta";
        Config.sys_log_dir = "/var/starrocks/log";
        when(os.getFileSystem()).thenReturn(fs);
        when(fs.getFileStores()).thenReturn(List.of(storeRoot));
        when(storeRoot.getMount()).thenReturn("/");
        when(storeRoot.getVolume()).thenReturn("/dev/sda");

        sm.init();

        // CPU
        List<Metric<?>> cpuMetrics = getByName("cpu");
        assertThat(cpuMetrics).hasSize(8);
        List<String> modes = new ArrayList<>();
        for (Metric<?> m : cpuMetrics) {
            modes.add(m.getLabels().get(0).getValue());
        }
        Collections.sort(modes);
        assertThat(modes).containsExactlyInAnyOrder("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal");

        // Memory
        List<Metric<?>> memMetrics = getByName("memory");
        assertThat(memMetrics).hasSize(4);
        List<String> memNames = new ArrayList<>();
        for (Metric<?> m : memMetrics) {
            memNames.add(m.getLabels().get(0).getValue());
        }
        Collections.sort(memNames);
        assertThat(memNames).containsExactlyInAnyOrder("total", "used", "swap_total", "swap_used");

        // NetIF
        for (String metricName : List.of(
                "network_receive_bytes", "network_receive_packets", "network_receive_errors", "network_receive_dropped",
                "network_send_bytes", "network_send_packets", "network_send_errors")) {
            List<Metric<?>> ms = getByName(metricName);
            assertThat(ms).hasSize(1);
            assertThat(ms.get(0).getLabels().get(0).getValue()).isEqualTo("eth0");
        }

        // Disk metrics registered for device "sda"
        List<String> expectedDiskMetricNames = List.of(
                "disk_total_capacity", "disk_used_capacity",
                "disk_reads_completed", "disk_bytes_read", "disk_read_time_ms",
                "disk_writes_completed", "disk_bytes_written", "disk_write_time_ms",
                "disk_io_time_ms", "disk_io_time_weighted_ms");
        for (String name : expectedDiskMetricNames) {
            List<Metric<?>> metrics = getByName(name);
            assertThat(metrics).hasSize(1);
            assertThat(metrics.get(0).getLabels().get(0).getValue()).isEqualTo("sda");
        }
    }

    @Test
    public void testUpdateCpuTicksSetsAllModes() {
        long[] ticks = new long[10];
        // Order by TickType index used in production code
        // USER(0), NICE(1), SYSTEM(2), IDLE(3), IOWAIT(4), IRQ(5), SOFTIRQ(6), STEAL(7)
        for (int i = 0; i < 8; i++) {
            ticks[i] = 100L + i;
        }

        when(cpu.getSystemCpuLoadTicks()).thenReturn(ticks);
        sm.init();
        sm.update();

        Map<String, Long> modeToValue = new HashMap<>();
        for (Metric<?> m : getByName("cpu")) {
            modeToValue.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        assertThat(modeToValue.get("user")).isEqualTo(100L);
        assertThat(modeToValue.get("nice")).isEqualTo(101L);
        assertThat(modeToValue.get("system")).isEqualTo(102L);
        assertThat(modeToValue.get("idle")).isEqualTo(103L);
        assertThat(modeToValue.get("iowait")).isEqualTo(104L);
        assertThat(modeToValue.get("irq")).isEqualTo(105L);
        assertThat(modeToValue.get("softirq")).isEqualTo(106L);
        assertThat(modeToValue.get("steal")).isEqualTo(107L);
    }

    @Test
    public void testUpdateMemoryAndSwap() {
        when(memory.getTotal()).thenReturn(1000L);
        when(memory.getAvailable()).thenReturn(250L);
        when(memory.getVirtualMemory()).thenReturn(vm);
        when(vm.getSwapTotal()).thenReturn(2048L);
        when(vm.getSwapUsed()).thenReturn(512L);
        sm.init();
        sm.update();

        Map<String, Long> nameToValue = new HashMap<>();
        for (Metric<?> m : getByName("memory")) {
            nameToValue.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        assertThat(nameToValue.get("total")).isEqualTo(1000L);
        assertThat(nameToValue.get("used")).isEqualTo(750L);
        assertThat(nameToValue.get("swap_total")).isEqualTo(2048L);
        assertThat(nameToValue.get("swap_used")).isEqualTo(512L);
    }

    @Test
    public void testUpdateIoSkipsWhenDiskstatsMissing() {
        // Register a disk so that update has something to try to update
        Config.meta_dir = "/var/starrocks/meta";
        Config.sys_log_dir = "/var/starrocks/log";
        when(os.getFileSystem()).thenReturn(fs);
        when(fs.getFileStores()).thenReturn(List.of(storeRoot));
        when(storeRoot.getMount()).thenReturn("/");
        when(storeRoot.getVolume()).thenReturn("/dev/sda");
        sm.init();

        try (MockedStatic<Files> filesMock = Mockito.mockStatic(Files.class)) {
            filesMock.when(() -> Files.notExists(argThat(p -> "/proc/diskstats".equals(p.toString())))).thenReturn(true);
            filesMock.when(() -> Files.notExists(argThat(p -> !"/proc/diskstats".equals(p.toString())))).thenReturn(false);
            // Should not throw
            sm.update();
        }

        // Disk metrics remain registered; values not asserted here
        assertThat(getByName("disk_reads_completed")).hasSize(1);
    }

    @Test
    public void testUpdateIoParsesValidDiskstatsLine() throws Exception {
        Config.meta_dir = "/var/starrocks/meta";
        Config.sys_log_dir = "/var/starrocks/log";
        when(os.getFileSystem()).thenReturn(fs);
        when(fs.getFileStores()).thenReturn(List.of(storeRoot));
        when(storeRoot.getMount()).thenReturn("/");
        when(storeRoot.getVolume()).thenReturn("/dev/sda");
        sm.init();

        try (MockedStatic<Files> filesMock = Mockito.mockStatic(Files.class);
                MockedConstruction<FileReader> frConstruction =
                        Mockito.mockConstruction(FileReader.class, (mockFr, context) -> {});
                MockedConstruction<BufferedReader> brConstruction = Mockito.mockConstruction(
                        BufferedReader.class, (mockBr, context) -> {
                            when(mockBr.readLine()).thenReturn(
                                    "   8       0 sda 100 0 200 300 400 0 500 600 0 700 800 900",
                                    (String) null);
                        })) {
            filesMock.when(() -> Files.notExists(any())).thenReturn(false);
            sm.update();
        }

        // Validate parsed and shifted values
        Map<String, Long> diskValues = new HashMap<>();
        for (Metric<?> m : getByName("disk_reads_completed")) {
            diskValues.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        assertThat(diskValues.get("sda")).isEqualTo(100L);

        Map<String, Long> bytesRead = new HashMap<>();
        for (Metric<?> m : getByName("disk_bytes_read")) {
            bytesRead.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        // sectors_read at parts[5] = 200 -> bytes = 200 << 9 = 102400
        assertThat(bytesRead.get("sda")).isEqualTo(200L << 9);

        Map<String, Long> bytesWritten = new HashMap<>();
        for (Metric<?> m : getByName("disk_bytes_written")) {
            bytesWritten.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        // sectors_written at parts[9] = 500 -> bytes = 500 << 9
        assertThat(bytesWritten.get("sda")).isEqualTo(500L << 9);

        Map<String, Long> ioTime = new HashMap<>();
        for (Metric<?> m : getByName("disk_io_time_ms")) {
            ioTime.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        assertThat(ioTime.get("sda")).isEqualTo(700L);

        Map<String, Long> ioTimeWeighted = new HashMap<>();
        for (Metric<?> m : getByName("disk_io_time_weighted_ms")) {
            ioTimeWeighted.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
        }
        assertThat(ioTimeWeighted.get("sda")).isEqualTo(800L);
    }

    @Test
    public void testUpdateNetworkAndTcp() throws Exception {
        when(os.getInternetProtocolStats()).thenReturn(ipStats);
        when(ipStats.getTCPv4Stats()).thenReturn(v4);
        when(ipStats.getTCPv6Stats()).thenReturn(v6);

        when(v4.getConnectionsActive()).thenReturn(1L);
        when(v6.getConnectionsActive()).thenReturn(10L);

        when(v4.getConnectionsPassive()).thenReturn(2L);
        when(v6.getConnectionsPassive()).thenReturn(20L);

        when(v4.getConnectionFailures()).thenReturn(3L);
        when(v6.getConnectionFailures()).thenReturn(30L);

        when(v4.getConnectionsReset()).thenReturn(4L);
        when(v6.getConnectionsReset()).thenReturn(40L);

        when(v4.getConnectionsEstablished()).thenReturn(5L);
        when(v6.getConnectionsEstablished()).thenReturn(50L);

        when(v4.getSegmentsReceived()).thenReturn(6L);
        when(v6.getSegmentsReceived()).thenReturn(60L);

        when(v4.getSegmentsSent()).thenReturn(7L);
        when(v6.getSegmentsSent()).thenReturn(70L);

        when(v4.getSegmentsRetransmitted()).thenReturn(8L);
        when(v6.getSegmentsRetransmitted()).thenReturn(80L);

        when(v4.getInErrors()).thenReturn(9L);
        when(v6.getInErrors()).thenReturn(90L);

        when(v4.getOutResets()).thenReturn(10L);
        when(v6.getOutResets()).thenReturn(100L);

        // NetIF discovery
        when(netIf.getName()).thenReturn("eth0");
        when(netIf.getIPv4addr()).thenReturn(new String[] {"127.0.0.1"});
        when(hal.getNetworkIFs(false)).thenReturn(List.of(netIf));

        try (MockedStatic<FrontendOptions> fo = Mockito.mockStatic(FrontendOptions.class)) {
            // IPv4-only
            fo.when(FrontendOptions::isBindIPV6).thenReturn(false);
            sm.init();

            when(netIf.getBytesRecv()).thenReturn(1_000L);
            when(netIf.getPacketsRecv()).thenReturn(200L);
            when(netIf.getInErrors()).thenReturn(3L);
            when(netIf.getInDrops()).thenReturn(4L);
            when(netIf.getBytesSent()).thenReturn(5_000L);
            when(netIf.getPacketsSent()).thenReturn(600L);
            when(netIf.getOutErrors()).thenReturn(7L);
            sm.update();
            Map<String, Long> v4Values = new HashMap<>();
            for (Metric<?> m : getByName("snmp")) {
                v4Values.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
            }
            assertThat(v4Values.get("active_opens")).isEqualTo(1L);
            assertThat(v4Values.get("passive_opens")).isEqualTo(2L);
            assertThat(v4Values.get("attempts_fails")).isEqualTo(3L);
            assertThat(v4Values.get("estab_resets")).isEqualTo(4L);
            assertThat(v4Values.get("curr_estab")).isEqualTo(5L);
            assertThat(v4Values.get("tcp_in_segs")).isEqualTo(6L);
            assertThat(v4Values.get("tcp_out_segs")).isEqualTo(7L);
            assertThat(v4Values.get("tcp_retrans_segs")).isEqualTo(8L);
            assertThat(v4Values.get("tcp_in_errs")).isEqualTo(9L);
            assertThat(v4Values.get("tcp_out_rsts")).isEqualTo(10L);
            // NetIF metric assertions
            assertThat(((GaugeMetricImpl<Long>) getByName("network_receive_bytes").get(0)).getValue()).isEqualTo(1_000L);
            assertThat(((GaugeMetricImpl<Long>) getByName("network_receive_packets").get(0)).getValue()).isEqualTo(200L);
            assertThat(((GaugeMetricImpl<Long>) getByName("network_receive_errors").get(0)).getValue()).isEqualTo(3L);
            assertThat(((GaugeMetricImpl<Long>) getByName("network_receive_dropped").get(0)).getValue()).isEqualTo(4L);
            assertThat(((GaugeMetricImpl<Long>) getByName("network_send_bytes").get(0)).getValue()).isEqualTo(5_000L);
            assertThat(((GaugeMetricImpl<Long>) getByName("network_send_packets").get(0)).getValue()).isEqualTo(600L);
            assertThat(((GaugeMetricImpl<Long>) getByName("network_send_errors").get(0)).getValue()).isEqualTo(7L);

            // IPv6 aggregation (v4 + v6)
            fo.when(FrontendOptions::isBindIPV6).thenReturn(true);
            sm.update();
            Map<String, Long> v6AggValues = new HashMap<>();
            for (Metric<?> m : getByName("snmp")) {
                v6AggValues.put(m.getLabels().get(0).getValue(), ((GaugeMetricImpl<Long>) m).getValue());
            }
            assertThat(v6AggValues.get("active_opens")).isEqualTo(11L);
            assertThat(v6AggValues.get("passive_opens")).isEqualTo(22L);
            assertThat(v6AggValues.get("attempts_fails")).isEqualTo(33L);
            assertThat(v6AggValues.get("estab_resets")).isEqualTo(44L);
            assertThat(v6AggValues.get("curr_estab")).isEqualTo(55L);
            assertThat(v6AggValues.get("tcp_in_segs")).isEqualTo(66L);
            assertThat(v6AggValues.get("tcp_out_segs")).isEqualTo(77L);
            assertThat(v6AggValues.get("tcp_retrans_segs")).isEqualTo(88L);
            assertThat(v6AggValues.get("tcp_in_errs")).isEqualTo(99L);
            assertThat(v6AggValues.get("tcp_out_rsts")).isEqualTo(110L);
        }
    }

    @SuppressWarnings("unchecked")
    private List<Metric<?>> getByName(SystemMetrics sm, String name) {
        return (List<Metric<?>>) (List<?>) sm.getRegistry().getMetricsByName(name);
    }

    @SuppressWarnings("unchecked")
    private List<Metric<?>> getByName(String name) {
        return getByName(sm, name);
    }
}
