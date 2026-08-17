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

package com.starrocks.common.util;

import com.starrocks.common.Pair;
import com.starrocks.thrift.TUnit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Compact binary serialisation for {@link RuntimeProfile} trees.
 *
 * <h3>Why a custom format?</h3>
 * Profiles are stored purely to produce a formatted display string on demand.
 * Both formatters ({@code DefaultProfileFormatter}, {@code JsonProfileFormatter})
 * consult exactly two {@link Counter} fields per counter entry:
 * <ul>
 *   <li>{@link Counter#getValue()} — the numeric value</li>
 *   <li>{@link Counter#getType()} — TUnit, controls how the value is printed</li>
 * </ul>
 * Merge-time fields (strategy, min/max, display_threshold) are never read during
 * formatting and are therefore intentionally omitted from the stored bytes.
 *
 * <h3>Dictionary compression</h3>
 * A six-zone nameId space eliminates repeated string encoding for both base names
 * and their {@code __MIN_OF_} / {@code __MAX_OF_} prefixed variants:
 * <pre>
 *   Zone 0  [0 .. S-1]         static base names
 *   Zone 1  [S .. 2S-1]        __MIN_OF_ + static base[id - S]
 *   Zone 2  [2S .. 3S-1]       __MAX_OF_ + static base[id - 2S]
 *   Zone 3  [3S .. 3S+L-1]     local base names
 *   Zone 4  [3S+L .. 3S+2L-1]  __MIN_OF_ + local base[id - (3S+L)]
 *   Zone 5  [3S+2L .. 3S+3L-1] __MAX_OF_ + local base[id - (3S+2L)]
 * </pre>
 * where S = {@code GLOBAL_COUNT} and L = {@code LOCAL_COUNT}.
 * Profile data is never persisted; IDs carry no cross-process meaning.
 *
 * <h3>Binary layout</h3>
 * <pre>
 * gzip(
 *   MAGIC        : 4 bytes  — 0x50524F46 ('P','R','O','F')
 *   VERSION      : 1 byte   — {@link #VERSION}
 *   GLOBAL_COUNT : 2 bytes  — ProfileKeyDictionary.STATIC_SIZE at write time (= S)
 *   LOCAL_COUNT  : 4 bytes  — number of local BASE names (= L; MIN/MAX not counted)
 *   LOCAL_NAMES  : L × string(baseName)
 *   NODES        : pre-order DFS, each NODE:
 *                    nodeName    : string
 *                    counters    : count:4 | COUNTER...
 *                    infoStrings : count:4 | (nameId:4, string value)...
 * where string = int(byteLen) + utf8Bytes  (no 65 535-byte limit)
 *                    children    : count:4 | NODE...
 *   COUNTER      : 17 bytes fixed
 *                    nameId   : 4 bytes  (zone as above)
 *                    parentId : 4 bytes
 *                    value    : 8 bytes
 *                    typeVal  : 1 byte   (TUnit ordinal)
 * )
 *
 * <p>All variable-length strings (node names, local base names, info-string values)
 * are encoded as {@code writeInt(byteLen) + write(utf8Bytes)} rather than
 * {@link java.io.DataOutputStream#writeUTF writeUTF}, which is limited to 65 535
 * bytes and throws {@link java.io.UTFDataFormatException} for long SQL text or
 * serialised session-variable blobs.</p>
 * </pre>
 */
final class ProfileSerializer {

    private static final int MAGIC = 0x50524F46;  // 'P','R','O','F'
    static final byte VERSION = 6;

    // -- public API -----------------------------------------------------------

    /**
     * Serialises {@code profile} to the compact binary format.
     *
     * <p>A two-pass approach is used: the first pass collects all local base
     * names (prefixes stripped); the second pass encodes each name to its
     * six-zone nameId.</p>
     */
    static byte[] serialize(RuntimeProfile profile) throws IOException {
        // Pass 1: collect local base names in encounter order.
        LinkedHashSet<String> localBaseSet = new LinkedHashSet<>();
        collectLocalBaseNames(profile, localBaseSet);

        // Build index: baseName → position (0-based within local base zone).
        LinkedHashMap<String, Integer> localBaseIdx = new LinkedHashMap<>(localBaseSet.size() * 2);
        for (String name : localBaseSet) {
            localBaseIdx.put(name, localBaseIdx.size());
        }
        int localCount = localBaseIdx.size();

        // Pass 2: build node tree with fully-resolved nameIds.
        ProfileRawNode root = buildNode(profile, localBaseIdx, localCount);

        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        try (DataOutputStream dos = new DataOutputStream(new GZIPOutputStream(baos))) {
            dos.writeInt(MAGIC);
            dos.writeByte(VERSION);
            dos.writeShort(ProfileKeyDictionary.STATIC_SIZE); // GLOBAL_COUNT = S
            dos.writeInt(localCount);                          // LOCAL_COUNT
            for (String baseName : localBaseSet) {
                writeString(dos, baseName);
            }
            writeNode(dos, root, localBaseIdx, localCount);
        }
        return baos.toByteArray();
    }

    /**
     * Deserialises bytes produced by {@link #serialize} back into a
     * display-ready {@link RuntimeProfile}.
     *
     * <p>{@link RuntimeProfile#computeTimeInProfile()} is called on the root
     * after reconstruction so that {@code localTimePercent} values are correct.</p>
     */
    static RuntimeProfile deserialize(byte[] data) throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new GZIPInputStream(new ByteArrayInputStream(data)))) {
            int magic = dis.readInt();
            if (magic != MAGIC) {
                throw new IOException("Invalid profile magic: 0x" + Integer.toHexString(magic));
            }
            int version = dis.readByte() & 0xFF;
            if (version != VERSION) {
                throw new IOException("Unsupported profile version: " + version);
            }

            int staticCount = dis.readShort() & 0xFFFF;
            if (staticCount > ProfileKeyDictionary.STATIC_SIZE) {
                throw new IOException(
                        "Profile was written with a newer global dictionary (globalCount=" + staticCount
                                + ", known=" + ProfileKeyDictionary.STATIC_SIZE + ")");
            }

            int localCount = dis.readInt();

            String[] localBaseNames = new String[localCount];
            for (int i = 0; i < localCount; i++) {
                localBaseNames[i] = readString(dis);
            }

            RuntimeProfile root = toProfile(
                    readNode(dis, staticCount, localCount, localBaseNames),
                    staticCount, localCount, localBaseNames);
            root.computeTimeInProfile();
            return root;
        }
    }

    // -- write path -----------------------------------------------------------

    /**
     * Pass 1 — collects every local base name reachable from {@code profile}.
     *
     * <p>If a name has a {@code __MIN_OF_} or {@code __MAX_OF_} prefix whose
     * base is not in the static dictionary, the base (not the prefixed form) is
     * added.  This ensures MIN/MAX variants share the same base slot in the local
     * zone and do not consume extra {@code LOCAL_NAMES} entries.</p>
     */
    private static void collectLocalBaseNames(RuntimeProfile profile,
                                              LinkedHashSet<String> out) {
        for (String name : profile.getCounterMap().keySet()) {
            collectLocalBaseName(name, out);
        }
        Map<String, String> infoStrings = profile.getInfoStrings();
        synchronized (infoStrings) {
            for (String key : infoStrings.keySet()) {
                collectLocalBaseName(key, out);
            }
        }
        for (Pair<RuntimeProfile, Boolean> child : profile.getChildList()) {
            collectLocalBaseNames(child.first, out);
        }
    }

    private static void collectLocalBaseName(String name, LinkedHashSet<String> out) {
        if (ProfileKeyDictionary.staticId(name) != ProfileKeyDictionary.ABSENT) {
            return;  // static — no local entry needed
        }
        String base = stripPrefix(name);
        if (ProfileKeyDictionary.staticId(base) != ProfileKeyDictionary.ABSENT) {
            return;  // prefixed-static — handled by zones 1/2
        }
        out.add(base);
    }

    /**
     * Strips a single {@code __MIN_OF_} or {@code __MAX_OF_} prefix if present;
     * returns the name unchanged otherwise.
     */
    private static String stripPrefix(String name) {
        if (name.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)) {
            return name.substring(RuntimeProfile.MERGED_INFO_PREFIX_MIN.length());
        }
        if (name.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
            return name.substring(RuntimeProfile.MERGED_INFO_PREFIX_MAX.length());
        }
        return name;
    }

    /**
     * Pass 2 — resolves a string key to its six-zone nameId.
     *
     * <p>Zones 0–2 (static base, MIN, MAX) are resolved with a single lookup
     * into {@link ProfileKeyDictionary#STATIC_MAP} which was pre-populated with
     * all three variants at class-load time.  Only local keys require prefix
     * stripping.</p>
     *
     * @param localBaseIdx position of each local base name (0-based within zone 3)
     * @param localCount   total number of local base names
     */
    private static int nameId(String name,
                              LinkedHashMap<String, Integer> localBaseIdx,
                              int localCount) {
        final int staticCount = ProfileKeyDictionary.STATIC_SIZE;

        // Zones 0, 1, 2: base and prefixed static names (all in STATIC_MAP).
        int id = ProfileKeyDictionary.staticId(name);
        if (id != ProfileKeyDictionary.ABSENT) {
            return id;
        }

        // Zones 4, 5: prefixed local names — strip prefix to find base slot.
        if (name.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MIN)) {
            Integer localPos = localBaseIdx.get(
                    name.substring(RuntimeProfile.MERGED_INFO_PREFIX_MIN.length()));
            if (localPos != null) {
                return 3 * staticCount + localCount + localPos;  // zone 4
            }
        } else if (name.startsWith(RuntimeProfile.MERGED_INFO_PREFIX_MAX)) {
            Integer localPos = localBaseIdx.get(
                    name.substring(RuntimeProfile.MERGED_INFO_PREFIX_MAX.length()));
            if (localPos != null) {
                return 3 * staticCount + 2 * localCount + localPos;  // zone 5
            }
        }

        // Zone 3: local base name.
        Integer localPos = localBaseIdx.get(name);
        if (localPos != null) {
            return 3 * staticCount + localPos;
        }

        // Should not happen if pass 1 was complete; fall back to zone 3.
        int pos = localBaseIdx.size();
        localBaseIdx.put(name, pos);
        return 3 * staticCount + pos;
    }

    /**
     * Resolves a nameId back to its string, using the six-zone layout.
     * Zones 0–2 reference the pre-built static arrays; zones 3–5 use
     * {@code localBaseNames} with arithmetic.
     */
    private static String resolveId(int id, int staticCount, int localCount, String[] localBaseNames) {
        if (id < 3 * staticCount) {
            return ProfileKeyDictionary.STATIC_ALL.get(id);
        }
        int local = id - 3 * staticCount;
        if (local < localCount) {
            return localBaseNames[local];
        }
        if (local < 2 * localCount) {
            return RuntimeProfile.MERGED_INFO_PREFIX_MIN + localBaseNames[local - localCount];
        }
        return RuntimeProfile.MERGED_INFO_PREFIX_MAX + localBaseNames[local - 2 * localCount];
    }

    /**
     * Pass 2 — builds a {@link ProfileRawNode} tree with nameIds fully resolved.
     *
     * <p>Counters are emitted in BFS order (parent before child) so that
     * {@link RuntimeProfile#addCounter} can be called unconditionally on the
     * read path.</p>
     */
    private static ProfileRawNode buildNode(RuntimeProfile profile,
                                            LinkedHashMap<String, Integer> localBaseIdx,
                                            int localCount) {
        List<ProfileRawCounter> counters = new ArrayList<>(profile.getCounterMap().size());
        Map<String, Set<String>> childCounterMap = profile.getChildCounterMap();

        Queue<String> queue = new ArrayDeque<>();
        queue.add(RuntimeProfile.ROOT_COUNTER);

        Counter totalTime = profile.getCounter(RuntimeProfile.TOTAL_TIME_COUNTER);
        if (totalTime != null) {
            int ttId = nameId(RuntimeProfile.TOTAL_TIME_COUNTER, localBaseIdx, localCount);
            counters.add(new ProfileRawCounter(ttId,
                    nameId(RuntimeProfile.ROOT_COUNTER, localBaseIdx, localCount),
                    totalTime.getValue(), totalTime.getType().getValue()));
            queue.add(RuntimeProfile.TOTAL_TIME_COUNTER);
        }

        while (!queue.isEmpty()) {
            String parentName = queue.poll();
            Set<String> childNames = childCounterMap.get(parentName);
            if (childNames == null) {
                continue;
            }
            int parentId = nameId(parentName, localBaseIdx, localCount);
            for (String childName : childNames) {
                Pair<Counter, String> pair = profile.getCounterPair(childName);
                if (pair == null) {
                    continue;
                }
                int childId = nameId(childName, localBaseIdx, localCount);
                counters.add(new ProfileRawCounter(childId, parentId,
                        pair.first.getValue(), pair.first.getType().getValue()));
                queue.add(childName);
            }
        }

        Map<String, String> info = new LinkedHashMap<>();
        Map<String, String> src = profile.getInfoStrings();
        synchronized (src) {
            info.putAll(src);
        }

        List<ProfileRawNode> children = new ArrayList<>(profile.getChildList().size());
        for (Pair<RuntimeProfile, Boolean> child : profile.getChildList()) {
            children.add(buildNode(child.first, localBaseIdx, localCount));
        }
        return new ProfileRawNode(profile.getName(), counters, info, children);
    }

    private static void writeNode(DataOutputStream dos, ProfileRawNode node,
                                  LinkedHashMap<String, Integer> localBaseIdx,
                                  int localCount) throws IOException {
        writeString(dos, node.name() != null ? node.name() : "");

        dos.writeInt(node.counters().size());
        for (ProfileRawCounter c : node.counters()) {
            dos.writeInt(c.nameId());
            dos.writeInt(c.parentId());
            dos.writeLong(c.value());
            dos.writeByte(c.typeVal());
        }

        dos.writeInt(node.infoStrings().size());
        for (Map.Entry<String, String> e : node.infoStrings().entrySet()) {
            dos.writeInt(nameId(e.getKey(), localBaseIdx, localCount));
            writeString(dos, e.getValue());
        }

        dos.writeInt(node.children().size());
        for (ProfileRawNode child : node.children()) {
            writeNode(dos, child, localBaseIdx, localCount);
        }
    }

    // -- read path ------------------------------------------------------------

    private static ProfileRawNode readNode(DataInputStream dis,
                                           int staticCount, int localCount,
                                           String[] localBaseNames) throws IOException {
        String name = readString(dis);

        int counterCount = dis.readInt();
        List<ProfileRawCounter> counters = new ArrayList<>(counterCount);
        for (int i = 0; i < counterCount; i++) {
            counters.add(new ProfileRawCounter(
                    dis.readInt(), dis.readInt(), dis.readLong(), dis.readByte() & 0xFF));
        }

        int infoCount = dis.readInt();
        Map<String, String> info = new LinkedHashMap<>(infoCount * 2);
        for (int i = 0; i < infoCount; i++) {
            info.put(resolveId(dis.readInt(), staticCount, localCount, localBaseNames), readString(dis));
        }

        int childCount = dis.readInt();
        List<ProfileRawNode> children = new ArrayList<>(childCount);
        for (int i = 0; i < childCount; i++) {
            children.add(readNode(dis, staticCount, localCount, localBaseNames));
        }
        return new ProfileRawNode(name, counters, info, children);
    }

    /**
     * Reconstructs a {@link RuntimeProfile} from a {@link ProfileRawNode}.
     *
     * <p>Counters are added in BFS order, so every parent exists before its
     * children are inserted — satisfying {@link RuntimeProfile#addCounter}'s
     * precondition without any extra check.</p>
     */
    private static RuntimeProfile toProfile(ProfileRawNode node,
                                            int staticCount, int localCount,
                                            String[] localBaseNames) {
        RuntimeProfile profile = new RuntimeProfile(node.name());
        for (ProfileRawCounter c : node.counters()) {
            String name = resolveId(c.nameId(), staticCount, localCount, localBaseNames);
            String parentName = resolveId(c.parentId(), staticCount, localCount, localBaseNames);
            TUnit type = TUnit.findByValue(c.typeVal());
            Counter counter = profile.addCounter(name, type, null, parentName);
            counter.setValue(c.value());
        }
        for (Map.Entry<String, String> e : node.infoStrings().entrySet()) {
            profile.addInfoString(e.getKey(), e.getValue());
        }
        for (ProfileRawNode child : node.children()) {
            profile.addChild(toProfile(child, staticCount, localCount, localBaseNames));
        }
        return profile;
    }

    // -- string I/O helpers ---------------------------------------------------

    /**
     * Writes a UTF-8 string as {@code int byteLen + raw bytes}.
     *
     * <p>Unlike {@link DataOutputStream#writeUTF}, this encoding has no
     * 65 535-byte limit and is therefore safe for long SQL text or serialised
     * session-variable blobs.</p>
     */
    private static void writeString(DataOutputStream dos, String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        dos.writeInt(bytes.length);
        dos.write(bytes);
    }

    /**
     * Reads a string previously written by {@link #writeString}.
     */
    private static String readString(DataInputStream dis) throws IOException {
        int len = dis.readInt();
        byte[] bytes = new byte[len];
        dis.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private ProfileSerializer() {
    }
}
