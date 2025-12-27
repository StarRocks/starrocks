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

package com.starrocks.extension;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Set;

public class ExtensionManager {
    private static final Logger LOG = LogManager.getLogger(ExtensionManager.class);

    private static final ExtensionManager INSTANCE = new ExtensionManager();

    private final ExtensionContext context = new DefaultExtensionContext();

    public static ExtensionManager getInstance() {
        return INSTANCE;
    }

    public static <T> T getCompoment(Class<T> clazz) {
        return getInstance().context.get(clazz);
    }

    public void loadExtensionsFromDir(String dirPath) {
        final JarScanner scanner = new JarScanner();
        LOG.info("start to load extensions");
        Set<Class<?>> extClasses = scanner.scanJarDirectory(dirPath);
        loadExtensions(extClasses);
        LOG.info("all extensions loaded finished");
    }
    public void loadExtensionsFromClassPath(String classPath) {
        final DirectoryClassPathScanner scanner = new DirectoryClassPathScanner();
        LOG.info("start to load extensions from classPath {}", classPath);
        Set<Class<?>> extClasses = scanner.scan(classPath);
        loadExtensions(extClasses);
        LOG.info("load extensions finished");
    }

    private void loadExtensions(Set<Class<?>> extClasses) {
        for (Class<?> clazz : extClasses) {
            try {
                SRModule annotation = clazz.getAnnotation(SRModule.class);
                if (annotation == null) {
                    continue;
                }

                StarRocksExtension ext = (StarRocksExtension) clazz.getDeclaredConstructor().newInstance();
                ext.onLoad(context);

                LOG.info("Loaded extension: {}", annotation.name());
            } catch (Exception e) {
                LOG.warn("Failed to load extension: {}", clazz.getName(), e);
            }
        }
    }
}
