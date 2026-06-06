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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/plugin/PluginMgr.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.plugin;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PrintableMap;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.UninstallPluginLog;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.plugin.PluginInfo.PluginType;
import com.starrocks.plugin.PluginLoader.PluginStatus;
import com.starrocks.qe.AuditLogBuilder;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InstallPluginStmt;
import com.starrocks.sql.ast.UninstallPluginStmt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class PluginMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(PluginMgr.class);

    public static final String BUILTIN_PLUGIN_PREFIX = "__builtin_";

    private final Map<String, PluginLoader>[] plugins;
    // all dynamic plugins should have unique names,
    private final Set<String> dynamicPluginNames;

    public PluginMgr() {
        plugins = new Map[PluginType.MAX_PLUGIN_TYPE_SIZE];
        for (int i = 0; i < PluginType.MAX_PLUGIN_TYPE_SIZE; i++) {
            plugins[i] = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        }
        dynamicPluginNames = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
    }

    // create the plugin dir if missing
    public void init() throws PluginException {
        File file = new File(Config.plugin_dir);
        if (file.exists() && !file.isDirectory()) {
            throw new PluginException("FE plugin dir " + Config.plugin_dir + " is not a directory");
        }

        if (!file.exists()) {
            if (!file.mkdir()) {
                throw new PluginException("failed to create FE plugin dir " + Config.plugin_dir);
            }
        }

        initBuiltinPlugins();
    }

    private boolean checkDynamicPluginNameExist(String name) {
        synchronized (dynamicPluginNames) {
            return dynamicPluginNames.contains(name);
        }
    }

    private boolean addDynamicPluginName(String name) {
        synchronized (dynamicPluginNames) {
            return dynamicPluginNames.add(name);
        }
    }

    private boolean removeDynamicPluginName(String name) {
        synchronized (dynamicPluginNames) {
            return dynamicPluginNames.remove(name);
        }
    }

    private void initBuiltinPlugins() {
        // AuditLog
        AuditLogBuilder auditLogBuilder = new AuditLogBuilder();
        if (!registerBuiltinPlugin(auditLogBuilder.getPluginInfo(), auditLogBuilder)) {
            LOG.warn("failed to register audit log builder");
        }

        // other builtin plugins
    }

    // install a plugin from user's command.
    // install should be successfully, or nothing should be left if failed to install.
    public PluginInfo installPlugin(InstallPluginStmt stmt) throws IOException, StarRocksException {
        Map<String, String> properties = stmt.getProperties();
        String md5sum = properties == null ? null : properties.get(DynamicPluginLoader.MD5SUM_KEY);
        PluginLoader pluginLoader = new DynamicPluginLoader(Config.plugin_dir, stmt.getPluginPath(), md5sum);
        pluginLoader.setStatus(PluginStatus.INSTALLING);

        try {
            PluginInfo info = pluginLoader.getPluginInfo();
            if (stmt.getProperties() != null) {
                info.setProperties(stmt.getProperties());
            }

            if (checkDynamicPluginNameExist(info.getName())) {
                throw new StarRocksException("plugin " + info.getName() + " has already been installed.");
            }

            // install plugin
            pluginLoader.install();
            pluginLoader.setStatus(PluginStatus.INSTALLED);

            // double check again
            if (checkDynamicPluginNameExist(info.getName())) {
                throw new StarRocksException("plugin " + info.getName() + " has already been installed.");
            }

            GlobalStateMgr.getCurrentState().getEditLog().logInstallPlugin(info, wal -> {
                addDynamicPluginName(info.getName());
                plugins[info.getTypeId()].put(info.getName(), pluginLoader);
            });
            LOG.info("install plugin {}", info.getName());
            return info;
        } catch (Throwable e) {
            pluginLoader.uninstall();
            throw e;
        }
    }

    public void uninstallPluginFromStmt(UninstallPluginStmt stmt) throws IOException, StarRocksException {
        String pluginName = stmt.getPluginName();
        int typeId = uninstallPlugin(pluginName);

        GlobalStateMgr.getCurrentState().getEditLog().logUninstallPlugin(new UninstallPluginLog(pluginName), wal -> {
            plugins[typeId].remove(pluginName);
            removeDynamicPluginName(pluginName);
        });
        LOG.info("uninstall plugin = {}", pluginName);
    }

    public void replayUninstallPlugin(UninstallPluginLog log) {
        String pluginName = log.getName();
        try {
            int typeId = uninstallPlugin(pluginName);

            // remove the plugin from manager
            plugins[typeId].remove(pluginName);
            removeDynamicPluginName(pluginName);
        } catch (Exception e) {
            LOG.warn("replay uninstall plugin failed.", e);
        }
    }

    /**
     * Dynamic uninstall plugin.
     * If uninstall failed, the plugin should NOT be removed from plugin manager.
     */
    public int uninstallPlugin(String name) throws IOException, StarRocksException {
        if (!checkDynamicPluginNameExist(name)) {
            throw new DdlException("Plugin " + name + " does not exist");
        }

        for (int i = 0; i < PluginType.MAX_PLUGIN_TYPE_SIZE; i++) {
            if (plugins[i].containsKey(name)) {
                PluginLoader loader = plugins[i].get(name);
                if (loader == null) {
                    // this is not a atomic operation, so even if containsKey() is true,
                    // we may still get null object by get() method
                    continue;
                }

                if (!loader.isDynamicPlugin()) {
                    throw new DdlException("Only support uninstall dynamic plugins");
                }

                loader.pluginUninstallValid();
                loader.setStatus(PluginStatus.UNINSTALLING);
                // uninstall plugin
                loader.uninstall();

                // uninstall succeed, remove the plugin
                loader.setStatus(PluginStatus.UNINSTALLED);
                return i;
            }
        }

        throw new DdlException("Plugin " + name + " does not exist");
    }

    /**
     * For built-in Plugin register
     */
    public boolean registerBuiltinPlugin(PluginInfo pluginInfo, Plugin plugin) {
        if (Objects.isNull(pluginInfo) || Objects.isNull(plugin) || Objects.isNull(pluginInfo.getType())
                || Strings.isNullOrEmpty(pluginInfo.getName())) {
            return false;
        }

        PluginLoader loader = new BuiltinPluginLoader(Config.plugin_dir, pluginInfo, plugin);
        PluginLoader checkLoader = plugins[pluginInfo.getTypeId()].putIfAbsent(pluginInfo.getName(), loader);

        return checkLoader == null;
    }

    /*
     * replay load plugin.
     * It must add the plugin to the "plugins" and "dynamicPluginNames", even if the plugin
     * is not loaded successfully.
     */
    public void replayLoadDynamicPlugin(PluginInfo info) throws IOException {
        DynamicPluginLoader pluginLoader = new DynamicPluginLoader(Config.plugin_dir, info);
        try {
            // should add to "plugins" first before loading.
            PluginLoader checkLoader = plugins[info.getTypeId()].putIfAbsent(info.getName(), pluginLoader);
            if (checkLoader != null) {
                throw new StarRocksException("plugin " + info.getName() + " has already been installed.");
            }

            pluginLoader.setStatus(PluginStatus.INSTALLING);
            // install plugin
            pluginLoader.reload();
            pluginLoader.setStatus(PluginStatus.INSTALLED);
        } catch (IOException | StarRocksException e) {
            pluginLoader.setStatus(PluginStatus.ERROR, e.getMessage());
            LOG.warn("fail to load plugin", e);
        } finally {
            // this is a replay process, so whether it is successful or not, add it's name.
            addDynamicPluginName(info.getName());
        }
    }

    public final Plugin getActivePlugin(String name, PluginType type) {
        PluginLoader loader = plugins[type.ordinal()].get(name);

        if (null != loader && loader.getStatus() == PluginStatus.INSTALLED) {
            return loader.getPlugin();
        }

        return null;
    }

    public final List<Plugin> getActivePluginList(PluginType type) {
        Map<String, PluginLoader> m = plugins[type.ordinal()];
        List<Plugin> l = Lists.newArrayListWithCapacity(m.size());

        m.values().forEach(d -> {
            if (d.getStatus() == PluginStatus.INSTALLED) {
                l.add(d.getPlugin());
            }
        });

        return Collections.unmodifiableList(l);
    }

    public final List<PluginInfo> getAllDynamicPluginInfo() {
        List<PluginInfo> list = Lists.newArrayList();
        for (Map<String, PluginLoader> map : plugins) {
            map.values().forEach(loader -> {
                try {
                    if (loader.isDynamicPlugin()) {
                        list.add(loader.getPluginInfo());
                    }
                } catch (Exception e) {
                    LOG.warn("load plugin from {} failed", loader.source, e);
                }
            });
        }

        return list;
    }

    public List<List<String>> getPluginShowInfos() {
        List<List<String>> rows = Lists.newArrayList();
        for (Map<String, PluginLoader> map : plugins) {
            for (Map.Entry<String, PluginLoader> entry : map.entrySet()) {
                List<String> r = Lists.newArrayList();
                PluginLoader loader = entry.getValue();

                PluginInfo pi = null;
                try {
                    pi = loader.getPluginInfo();
                } catch (Exception e) {
                    // plugin may not be loaded successfully
                    LOG.warn("failed to get plugin info for plugin: {}", entry.getKey(), e);
                }

                r.add(entry.getKey());
                r.add(pi != null ? pi.getType().name() : "UNKNOWN");
                r.add(pi != null ? pi.getDescription() : "UNKNOWN");
                r.add(pi != null ? pi.getVersion().toString() : "UNKNOWN");
                r.add(pi != null ? pi.getJavaVersion().toString() : "UNKNOWN");
                r.add(pi != null ? pi.getClassName() : "UNKNOWN");
                r.add(pi != null ? pi.getSoName() : "UNKNOWN");
                if (Strings.isNullOrEmpty(loader.source)) {
                    r.add("Builtin");
                } else {
                    r.add(loader.source);
                }

                r.add(loader.getStatus().toString());
                r.add(pi != null ?
                        "{" + new PrintableMap<>(pi.getProperties(), "=", true, false, true).toString() + "}" :
                        "UNKNOWN");
                rows.add(r);
            }
        }
        return rows;
    }




    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<PluginInfo> pluginInfos = getAllDynamicPluginInfo();

        int numJson = 1 + pluginInfos.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.PLUGIN_MGR, numJson);
        writer.writeInt(pluginInfos.size());
        for (PluginInfo pluginInfo : pluginInfos) {
            writer.writeJson(pluginInfo);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(PluginInfo.class, this::replayLoadDynamicPlugin);
    }
}
