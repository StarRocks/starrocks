// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;

import com.starrocks.common.Config;
import com.starrocks.common.util.NetUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;



public class FrontendOptionsTest {

    @Mocked
    InetAddress addr;

    private boolean useFqdn = true;
    private boolean useFqdnFile = true;

    @Before
    public void setUp() {
        Config.enable_fqdn_func = true;
    }

    @Test
    public void cidrTest() {

        List<String> priorityCidrs = FrontendOptions.priorityCidrs;
        priorityCidrs.add("192.168.5.136/32");

        FrontendOptions frontendOptions = new FrontendOptions();
        boolean inPriorNetwork = frontendOptions.isInPriorNetwork("127.0.0.1");
        Assert.assertEquals(false, inPriorNetwork);

        inPriorNetwork = frontendOptions.isInPriorNetwork("192.168.5.136");
        Assert.assertEquals(true, inPriorNetwork);

    }

    private void mockNet() {
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return addr;
            }
            @Mock
            public String getHostAddress() {
                return "127.0.0.10";
            }
            @Mock
            public String getCanonicalHostName() {
                return "sandbox";
            }
        };
        new MockUp<NetUtils>() {
            @Mock
            public List<InetAddress> getHosts() {
                List<InetAddress> hosts = new ArrayList<>();
                hosts.add(addr);
                return hosts;
            }
        };

        new MockUp<FrontendOptions>() {
            @Mock
            public void initAddrUseFqdn(List<InetAddress> hosts) throws UnknownHostException {
                useFqdn = true;
                useFqdnFile = true;
            }
            @Mock
            public void initAddrUseIp(List<InetAddress> hosts) {
                useFqdn = false;
                useFqdnFile = false;
            }
        };
    }

    @Test
    public void enableFQDNTest() throws UnknownHostException, 
                                        NoSuchFieldException, 
                                        SecurityException, 
                                        IllegalArgumentException, 
                                        IllegalAccessException {
        mockNet();
        Field field = FrontendOptions.class.getDeclaredField("localAddr");
        field.setAccessible(true);
        field.set(null, addr);
        Field field1 = FrontendOptions.class.getDeclaredField("useFqdn");
        field1.setAccessible(true);
        
        field1.set(null, true);
        Assert.assertTrue(FrontendOptions.getLocalHostAddress().equals("sandbox"));
        field1.set(null, false);
        Assert.assertTrue(FrontendOptions.getLocalHostAddress().equals("127.0.0.10"));
    }

    @Test 
    public void testChooseHostType() throws UnknownHostException {
        mockNet();
        useFqdn = true;
        FrontendOptions.init(new String[]{"-host_type", "ip"});
        Assert.assertTrue(!useFqdn);
        useFqdn = false;
        FrontendOptions.init(new String[]{"-host_type", "fqdn"});
        Assert.assertTrue(useFqdn);
        useFqdn = false;
        FrontendOptions.init(new String[]{});
        Assert.assertTrue(useFqdn);
    }

    
    private void mkdir(boolean hasFqdn, String metaPath) {
        File dir = new File(metaPath);
        if (!dir.exists()) {
            dir.mkdir();
        } else {
            deleteDir(dir);
        }
        File dirImage = new File(metaPath + "image/");
        dirImage.mkdir();
        File version = new File(metaPath + "image/ROLE");
        try {
            version.createNewFile();
            String line1 = "#Mon Feb 02 13:59:54 CST 2022\n";
            String line2 = "hostType=FQDN\n";
            String line3 = "name=172.26.92.139_3114_1652846479257\n";
            String line4 = "role=FOLLOWER\n";
            FileWriter fw = new FileWriter(version);
            fw.write(line3);
            fw.write(line4);
            fw.write(line1);
            if (hasFqdn) {
                fw.write(line2);    
            }
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void deleteDir(File dir) {
        
        if (!dir.exists()) {
            return;
        }
        if (dir.isFile()) {
            dir.delete();
        } else {
            for (File file : dir.listFiles()) {
                deleteDir(file);
            }
        }
        dir.delete();
    }

    @Test
    public void testChooseHostTypeByFile() throws UnknownHostException {
        mockNet();
        Config.meta_dir = "feOpTestDir1";
        String metaPath = Config.meta_dir + "/";
        // fqdn
        mkdir(true, metaPath);
        useFqdnFile = false;
        FrontendOptions.init(new String[]{});
        Assert.assertTrue(useFqdnFile);
        File dir = new File(metaPath);
        deleteDir(dir);
        // ip
        mkdir(false, metaPath);
        useFqdnFile = true;
        FrontendOptions.init(new String[]{});
        Assert.assertTrue(!useFqdnFile);
        dir = new File(metaPath);
        deleteDir(dir);
    }

    private void testInitAddrUseFqdnCommonMock() {
        new MockUp<System>() {
            @Mock
            public void exit(int status) throws IllegalAccessException {
                throw new IllegalAccessException();
            }
        };
        new MockUp<NetUtils>() {
            @Mock
            public List<InetAddress> getHosts() {
                List<InetAddress> hosts = new ArrayList<>();
                hosts.add(addr);
                return hosts;
            }
        };
    }

    @Test(expected = IllegalAccessException.class)
    public void testInitAddrUseFqdnNullAddr() throws UnknownHostException {
        testInitAddrUseFqdnCommonMock();
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return null;
            }
        };
        List<InetAddress> hosts = NetUtils.getHosts();
        // InetAddress uncheckedLocalAddr = InetAddress.getLocalHost(); uncheckedLocalAddr will be null,
        // so this case will trigger the IllegalAccessException
        // which has mocked in testInitAddrUseFqdnCommonMock [System.exit()]
        FrontendOptions.initAddrUseFqdn(hosts);
    }

    @Test(expected = IllegalAccessException.class)
    public void testInitAddrUseFqdnNullCanonicalHostName() throws UnknownHostException {
        testInitAddrUseFqdnCommonMock();
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return addr;
            }
            @Mock
            public String getHostAddress() {
                return "127.0.0.10";
            }
            @Mock
            public String getCanonicalHostName() {
                return null;
            }
        };
        List<InetAddress> hosts = NetUtils.getHosts();
        // String uncheckedFqdn = uncheckedLocalAddr.getCanonicalHostName(); uncheckedFqdn will be null,
        // so this case will trigger the IllegalAccessException 
        // which has mocked in testInitAddrUseFqdnCommonMock [System.exit()]
        FrontendOptions.initAddrUseFqdn(hosts);
    }

    @Test
    public void testInitAddrUseFqdn() throws UnknownHostException, NoSuchFieldException, SecurityException,
            IllegalArgumentException, IllegalAccessException {
        testInitAddrUseFqdnCommonMock();
        new MockUp<InetAddress>() {
            @Mock
            public InetAddress getLocalHost() throws UnknownHostException {
                return addr;
            }
            @Mock
            public String getHostAddress() {
                return "127.0.0.10";
            }
            @Mock
            public String getCanonicalHostName() {
                return "sandbox";
            }
            @Mock
            public InetAddress getByName(String host) throws UnknownHostException {
                return addr;
            }
        };
        List<InetAddress> hosts = NetUtils.getHosts();
        FrontendOptions.initAddrUseFqdn(hosts);
        Field field = FrontendOptions.class.getDeclaredField("localAddr");
        field.setAccessible(true);
        InetAddress addr1 = (InetAddress) field.get(FrontendOptions.class);
        Assert.assertTrue(addr1.equals(addr));
    }


    @Test
    public void testSaveStartType() throws FileNotFoundException, IOException {
        Config.meta_dir = "feOpTestDir2";
        String metaPath = Config.meta_dir + "/";
        // fqdn
        File dir = new File(metaPath);
        deleteDir(dir);
        mkdir(false, metaPath);
        FrontendOptions.saveStartType();
        String roleFilePath = Config.meta_dir + "/image/ROLE";
        File roleFile = new File(roleFilePath);
        
        Properties prop = new Properties();
        String hostType;
        try (FileInputStream in = new FileInputStream(roleFile)) {
            prop.load(in);
        }
        hostType = prop.getProperty("hostType", null);
        Assert.assertTrue((hostType.equals("IP") || hostType.equals("FQDN")));
        dir = new File(metaPath);
        deleteDir(dir);
    }
}
