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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/load/loadv2/SparkYarnConfigFiles.java

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

package com.starrocks.load.loadv2;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

public class SparkYarnConfigFiles {
    private static final Logger LOG = LogManager.getLogger(SparkYarnConfigFiles.class);

    private static final String HADOOP_CONF_FILE = "core-site.xml";
    private static final String YARN_CONF_FILE = "yarn-site.xml";
    private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";
    private static final String HADOOP_PREFIX = "hadoop.";
    private static final String YARN_PREFIX = "yarn.";

    private final String configDir;
    private final List<ConfigFile> configFiles;

    public String getConfigDir() {
        return this.configDir;
    }

    public SparkYarnConfigFiles(String resourceName, Map<String, String> properties) {
        this.configDir = Config.yarn_config_dir + "/" + resourceName;
        this.configFiles = Lists.newArrayList();
        createConfigFiles(properties);
    }

    // for unit test
    public SparkYarnConfigFiles(String resourceName, String parentDir, Map<String, String> properties) {
        this.configDir = parentDir + "/" + resourceName;
        this.configFiles = Lists.newArrayList();
        createConfigFiles(properties);
    }

    private void createConfigFiles(Map<String, String> properties) {
        LOG.info("create config file, properties size: {}", properties.size());
        configFiles.add(new XMLConfigFile(configDir + "/" + HADOOP_CONF_FILE,
                getPropertiesByPrefix(properties, HADOOP_PREFIX)));
        configFiles.add(new XMLConfigFile(configDir + "/" + YARN_CONF_FILE,
                getPropertiesByPrefix(properties, YARN_PREFIX)));
    }

    public void prepare() throws LoadException {
        initConfigFile();
    }

    private void initConfigFile() throws LoadException {
        LOG.info("start to init config file. config dir: {}", this.configDir);
        Preconditions.checkState(!Strings.isNullOrEmpty(configDir));

        boolean needUpdate = false;
        boolean needReplace = false;
        CHECK:
        {
            if (!checkConfigDirExists(this.configDir)) {
                needUpdate = true;
                break CHECK;
            }

            for (ConfigFile configFile : configFiles) {
                String filePath = configFile.getFilePath();
                if (!checkConfigFileExists(filePath)) {
                    needUpdate = true;
                    needReplace = true;
                    break CHECK;
                }
            }
        }

        if (needUpdate) {
            updateConfig(needReplace);
        }
        LOG.info("init spark yarn config success, config dir={}, config file size={}",
                configDir, configFiles.size());
    }

    private boolean checkConfigDirExists(String dir) {
        boolean result = true;
        File configDir = new File(dir);
        if (!configDir.exists() || !configDir.isDirectory()) {
            result = false;
        }
        LOG.info("check yarn client config dir exists, result: {}", result);
        return result;
    }

    private boolean checkConfigFileExists(String filePath) {
        boolean result = true;
        File configFile = new File(filePath);
        if (!configFile.exists() || !configFile.isFile()) {
            result = false;
        }
        LOG.info("check yarn client config file path exists, result: {}, path: {}", result, filePath);
        return result;
    }

    private void updateConfig(boolean needReplace) throws LoadException {
        if (needReplace) {
            clearAndDelete(this.configDir);
        }
        mkdir(this.configDir);
        for (ConfigFile configFile : configFiles) {
            configFile.createFile();
        }
        LOG.info("finished to update yarn client config dir, dir={}", configDir);
    }

    private Map<String, String> getPropertiesByPrefix(Map<String, String> properties, String prefix) {
        Map<String, String> result = Maps.newHashMap();
        Iterator<Map.Entry<String, String>> iterator = properties.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> property = iterator.next();
            String key = property.getKey();
            if (key.startsWith(SPARK_HADOOP_PREFIX)) {
                String newKey = key.substring(SPARK_HADOOP_PREFIX.length());
                if (newKey.startsWith(prefix)) {
                    result.put(newKey, property.getValue());
                    iterator.remove();
                }
            }
        }
        return result;
    }

    private void clearAndDelete(String deletePath) {
        File file = new File(deletePath);
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            if (!file.delete()) {
                LOG.warn("Failed to delete file, filepath={}", file.getAbsolutePath());
            }
            return;
        }
        File[] files = file.listFiles();
        if (files != null) {
            for (File file1 : files) {
                clearAndDelete(file1.getAbsolutePath());
            }
        }
        if (!file.delete()) {
            LOG.warn("Failed to delete directory, directoryPath={}", file.getAbsolutePath());
        }
    }

    private void mkdir(String configDir) {
        File file = new File(configDir);
        if (!file.mkdirs()) {
            LOG.warn("Failed to create directory, directoryPath={}", file.getAbsolutePath());
        }
    }

    // xml config file
    public static class XMLConfigFile implements ConfigFile {
        private static final String CONFIGURATION = "configuration";
        private static final String PROPERTY = "property";
        private static final String NAME = "name";
        private static final String VALUE = "value";

        private final String filePath;
        private final Map<String, String> configProperties;

        public XMLConfigFile(String filePath, Map<String, String> configProperties) {
            this.filePath = filePath;
            this.configProperties = configProperties;
        }

        @Override
        public String getFilePath() {
            return filePath;
        }

        @Override
        public void createFile() throws LoadException {
            createXML(this.filePath, this.configProperties);
        }

        private void createXML(String filePath, Map<String, String> properties) throws LoadException {
            try {
                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = factory.newDocumentBuilder();
                Document document = db.newDocument();
                document.setXmlStandalone(true);
                Element configuration = (Element) appendNode(document, CONFIGURATION, null);
                for (Map.Entry<String, String> pair : properties.entrySet()) {
                    Element property = (Element) appendNode(configuration, PROPERTY, null);
                    appendNode(property, NAME, pair.getKey());
                    appendNode(property, VALUE, pair.getValue());
                }

                TransformerFactory tff = TransformerFactory.newInstance();
                // For sonar issue: XML parsers should not be vulnerable to XXE attacks
                tff.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                tff.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
                Transformer tf = tff.newTransformer();

                tf.setOutputProperty(OutputKeys.INDENT, "yes");
                tf.transform(new DOMSource(document), new StreamResult(new File(filePath)));
            } catch (Exception e) {
                throw new LoadException(e.getMessage());
            }
        }

        private Node appendNode(Node parent, String tag, String content) {
            Element child;
            if (parent instanceof Document) {
                child = ((Document) parent).createElement(tag);
            } else {
                child = parent.getOwnerDocument().createElement(tag);
            }
            if (content != null && !content.equals("")) {
                child.setTextContent(content);
            }
            return parent.appendChild(child);
        }
    }
}
