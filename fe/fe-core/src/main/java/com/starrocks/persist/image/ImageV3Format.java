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

package com.starrocks.persist.image;

import java.nio.charset.StandardCharsets;

/**
 * Constants of the format-v3 image container. The on-disk layout is
 *
 * <pre>
 *   [MAGIC 8B][ImageHeaderPB, delimited][section 1]...[section N][FooterPB][4B len(FooterPB)][4B crc32]
 * </pre>
 *
 * where the two trailing integers are big-endian and the crc32 covers {@code [0, EOF-4)}.
 * Everything else about the layout is documented in {@code persist/image.proto}.
 */
public final class ImageV3Format {
    /** Format discriminator at file offset 0. */
    public static final byte[] MAGIC = "SRIMAGE3".getBytes(StandardCharsets.US_ASCII);
    /** {@code [4B len(FooterPB)][4B crc32]}. */
    public static final int TRAILER_LENGTH = 8;
    /** Value written into every {@code IndexEntryPB.version} today. */
    public static final int CURRENT_SECTION_VERSION = 0;
    /** Highest {@code IndexEntryPB.version} this code understands; newer sections are skipped. */
    public static final int SUPPORTED_SECTION_VERSION = 0;
    /** A manager message larger than this is a design smell; the writer logs a warning. */
    public static final long MANAGER_MESSAGE_SOFT_LIMIT_BYTES = 64L << 20;
    /** zstd's own default level, also the level the design benchmark was measured with. */
    public static final int DEFAULT_ZSTD_LEVEL = 3;

    private ImageV3Format() {
    }
}
