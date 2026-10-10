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

package com.starrocks.connector.lance;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URLClassLoader;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class LanceDirectoryCatalogTest {
    private static final String CLIENT_CLASS = "com.starrocks.lance.metadata.LanceDirectoryNamespace";

    @Test
    void keepsSuccessfulLoaderOpenForSubsequentSdkCalls() throws Exception {
        URLClassLoader loader = mock(URLClassLoader.class);
        doReturn(String.class).when(loader).loadClass(CLIENT_CLASS);

        assertSame(String.class, LanceDirectoryCatalog.loadClientClass(loader));
        verify(loader, never()).close();
    }

    @Test
    void closesLoaderWhenClientClassIsMissing() throws Exception {
        URLClassLoader loader = mock(URLClassLoader.class);
        doThrow(new ClassNotFoundException(CLIENT_CLASS)).when(loader).loadClass(CLIENT_CLASS);

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> LanceDirectoryCatalog.loadClientClass(loader));

        assertEquals("Install the Lance metadata libraries in FE lib/lance-metadata-lib", failure.getMessage());
        verify(loader).close();
    }

    @Test
    void closesLoaderOnLinkageFailureAndPreservesCloseFailure() throws Exception {
        URLClassLoader loader = mock(URLClassLoader.class);
        doThrow(new NoClassDefFoundError("missing dependency")).when(loader).loadClass(CLIENT_CLASS);
        IOException closeFailure = new IOException("close failed");
        doThrow(closeFailure).when(loader).close();

        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> LanceDirectoryCatalog.loadClientClass(loader));

        assertEquals(1, failure.getSuppressed().length);
        assertSame(closeFailure, failure.getSuppressed()[0]);
        verify(loader).close();
    }
}
