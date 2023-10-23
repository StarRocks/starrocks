package com.starrocks.persist;

import com.starrocks.common.io.Writable;
import org.junit.Test;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class PersistTest {
    @Test
    public void testEmptyConstructorOfWritableSubClasses() throws Exception {
        String basePackage = Writable.class.getClassLoader().getResource("").getPath();
        File[] files = new File(basePackage).listFiles();
        List<String> allClassPaths = new ArrayList<>();
        for (File file : files) {
            if (file.isDirectory()) {
                listPackages(file.getName(), allClassPaths);
            }
        }

        for (String classPath : allClassPaths) {
            Class<?> clazz = Class.forName(classPath);
            if (clazz.getSuperclass() == null) {
                continue;
            }

            for (Class<?> superClazz : clazz.getInterfaces()) {
                if (superClazz.equals(Writable.class) && !clazz.isAnonymousClass()) {
                    try {
                        clazz.newInstance();
                    } catch (Throwable t) {
                        System.out.println("class : " + classPath + " should have empty constructor");
                    }
                }
            }
        }
    }

    public void listPackages(String basePackage, List<String> classes) {
        URL url = Writable.class.getClassLoader()
                .getResource("./" + basePackage.replaceAll("\\.", "/"));
        File directory = new File(url.getFile());
        for (File file : directory.listFiles()) {
            if (file.isDirectory()) {
                listPackages(basePackage + "." + file.getName(), classes);
            } else {
                String classpath = file.getName();
                if (classpath.endsWith(".class")) {
                    classes.add(basePackage + "." + classpath.substring(0, classpath.length() - ".class".length()));
                }
            }
        }
    }
}
