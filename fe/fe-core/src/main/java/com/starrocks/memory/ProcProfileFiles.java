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

package com.starrocks.memory;

/**
 * The on-disk naming contract for FE process profiles, shared by {@link ProcProfileCollector}, which writes
 * them, and {@link ProcProfileCleaner}, which reaps them from a different thread.
 *
 * <p>A published profile is named {@code <prefix><yyyyMMdd-HHmmss>.html.tar.gz}. A collection in flight also
 * leaves the raw {@code .html} the profiler is still writing and, briefly, a {@code .tar.gz.tmp} archive.
 * Neither ends in {@link #SERVED_SUFFIX}, which is what {@code ProcProfileAction} and
 * {@code ProcProfileFileAction} filter on, so neither can be listed or downloaded half-written.
 *
 * <p>Only the format string is shared, not a formatter: {@link java.text.SimpleDateFormat} is not
 * thread-safe and the collector and the cleaner run on separate daemon threads.
 */
final class ProcProfileFiles {
    static final String CPU_FILE_NAME_PREFIX = "cpu-profile-";
    static final String MEM_FILE_NAME_PREFIX = "mem-profile-";
    static final String PUBLISHED_SUFFIX = ".html.tar.gz";
    /**
     * The suffix the HTTP endpoints select on. Deliberately broader than {@link #PUBLISHED_SUFFIX}: the
     * retention budget has to cover exactly the set of files those endpoints can serve, no more and no less.
     */
    static final String SERVED_SUFFIX = ".tar.gz";
    static final String TIME_FORMAT = "yyyyMMdd-HHmmss";

    private ProcProfileFiles() {
    }

    /**
     * Returns the timestamp portion of a profile file name, whether the file is published or still being
     * written, or null when the name is not a profile at all. Timestamps are fixed-width, so they sort
     * chronologically as strings and can be compared against a formatted cutoff directly.
     */
    static String profileTimePart(String fileName) {
        int prefixLength;
        if (fileName.startsWith(CPU_FILE_NAME_PREFIX)) {
            prefixLength = CPU_FILE_NAME_PREFIX.length();
        } else if (fileName.startsWith(MEM_FILE_NAME_PREFIX)) {
            prefixLength = MEM_FILE_NAME_PREFIX.length();
        } else {
            return null;
        }

        int dotIndex = fileName.indexOf('.', prefixLength);
        if (dotIndex <= prefixLength) {
            return null;
        }
        return fileName.substring(prefixLength, dotIndex);
    }

    /** True for a completed archive, i.e. one that the HTTP endpoints will list and serve. */
    static boolean isPublishedArchive(String fileName) {
        return fileName.endsWith(SERVED_SUFFIX);
    }
}
