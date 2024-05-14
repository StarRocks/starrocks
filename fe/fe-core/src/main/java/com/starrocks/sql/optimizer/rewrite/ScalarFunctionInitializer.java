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
package com.starrocks.sql.optimizer.rewrite;

import com.starrocks.catalog.DynamicScalarFunction;
import com.starrocks.catalog.StarrocksTypeConverter;
import com.starrocks.catalog.Type;
import com.starrocks.common.UDFException;
import com.starrocks.common.type.StarrocksType;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class ScalarFunctionInitializer {
    public static Type initialize(DynamicScalarFunction f, Object[] argsValues) {
        try {
            URL[] urls = {new URL("jar:" + f.getLocation() + "!/")};
            try (URLClassLoader classLoader = URLClassLoader.newInstance(urls)) {
                Class c = classLoader.loadClass(f.getSymbolName());

                for (Method m : c.getMethods()) {
                    if (!m.getDeclaringClass().equals(c)) {
                        continue;
                    }
                    String name = m.getName();
                    if (name.equalsIgnoreCase("initialize")) {
                        Object fnInstance = c.newInstance();
                        return StarrocksTypeConverter.getTypeFromSR((StarrocksType) m.invoke(fnInstance,
                                StarrocksTypeConverter.getSRTypes(f.getArgs()), argsValues));
                    }
                }
            } catch (IOException e) {
                throw new UDFException("Failed to load object_file: " + f.getLocation());
            } catch (ClassNotFoundException e) {
                throw new UDFException("Class '" + f.getSymbolName() + "' not found in object_file :"
                        + f.getLocation());
            } catch (IllegalAccessException | InstantiationException e) {
                throw new UDFException("Failed to create function " + f.getFunctionName().getFunction()
                        + ": " + f.getLocation(), e);
            } catch (InvocationTargetException e) {
                throw new UDFException("Failed to initialize function " + f.getFunctionName().getFunction()
                        + ": " + f.getLocation(), e);
            }
        } catch (MalformedURLException e) {
            throw new UDFException("Object file is invalid: " + f.getLocation(), e);
        }

        return null;
    }
}
