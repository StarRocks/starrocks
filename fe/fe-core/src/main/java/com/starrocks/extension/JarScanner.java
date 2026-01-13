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

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class JarScanner {
    private static final Logger LOG = LogManager.getLogger(JarScanner.class);

    public Set<Class<?>> scanJarDirectory(String dirPath) {
        Set<Class<?>> extensionClasses = new HashSet<>();
        File dir = new File(dirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            return extensionClasses;
        }

        // find the jar files
        File[] files = dir.listFiles((d, name) -> name.endsWith("ext.jar"));
        if (files == null) {
            return extensionClasses;
        }

        for (File jarFile : files) {
            try {
                extensionClasses.addAll(scanJar(jarFile));
            } catch (Exception e) {
                LOG.warn("Failed to scan jar: {}", jarFile, e);
            }
        }
        return extensionClasses;
    }

    private Set<Class<?>> scanJar(File jarFile) throws IOException, ClassNotFoundException {
        Set<Class<?>> classes = new HashSet<>();
        try (JarFile jar = new JarFile(jarFile)) {
            Enumeration<JarEntry> entries = jar.entries();
            URL[] urls = { new URL("jar:file:" + jarFile.getAbsolutePath() + "!/") };
            try (URLClassLoader cl = URLClassLoader.newInstance(urls)) {
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    if (entry.isDirectory() || !entry.getName().endsWith(".class")) {
                        continue;
                    }

                    String className = entry.getName().replace("/", ".").replace(".class", "");
                    Class<?> clazz = cl.loadClass(className);

                    if (StarRocksExtension.class.isAssignableFrom(clazz) && clazz.isAnnotationPresent(SRModule.class)) {
                        classes.add(clazz);
                    }
                }
            }
        }
        return classes;
    }
}
