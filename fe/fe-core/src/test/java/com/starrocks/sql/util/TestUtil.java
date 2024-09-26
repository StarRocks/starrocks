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

package com.starrocks.sql.util;

import org.apache.commons.math3.util.Pair;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class TestUtil {
    @Test
    public void testBox() {
        String s0 = "abcd";
        String s1 = new String(new byte[] {'a', 'b', 'c', 'd'});
        Box<String> b0 = Box.of(s0);
        Box<String> b1 = Box.of(s1);
        Box<String> b2 = Box.of(s0);
        Assert.assertEquals(b0, b2);
        Assert.assertEquals(b0.unboxed(), b2.unboxed());
        Assert.assertEquals(b0.hashCode(), b2.hashCode());
        Assert.assertNotEquals(b1, b2);
        Assert.assertEquals(b1.unboxed(), b2.unboxed());
        Assert.assertNotEquals(b1.hashCode(), b2.hashCode());
    }

    @Test
    public void testEitherOr() {
        EitherOr<String, String> a = EitherOr.left("abcd");
        EitherOr<String, String> b = EitherOr.right("abcd");
        Assert.assertEquals(a.left(), b.right());
        Assert.assertTrue(a.getFirst().isPresent());
        Assert.assertFalse(a.getSecond().isPresent());
        Assert.assertFalse(b.getFirst().isPresent());
        Assert.assertTrue(b.getSecond().isPresent());
    }

    @Test
    public void testTieredList() {
        TieredList<String> l0 = TieredList.<String>genesis();
        Assert.assertTrue(l0.isEmpty());
        TieredList.Builder<String> l1Builder = TieredList.<String>newGenesisTier();
        l1Builder.add("a", "b");
        l1Builder.add("c");
        l1Builder.addAll(Lists.newArrayList("d", "e", "f"));
        TieredList<String> l1 = l1Builder.build();
        l0 = l0.concat(l1Builder.build());

        Assert.assertTrue(l1.size() == 6);
        Assert.assertTrue(l0.size() == 6);

        l0 = l0.concat(Collections.emptyList());
        Assert.assertTrue(l0.size() == 6);
        TieredList<String> l01 = l0.concat(l0);
        Assert.assertTrue(l01.size() == 12);

        l0 = l0.newTier().add("g").build();
        l1 = l1.concatOne("k");
        Assert.assertTrue(l1.size() == 7);
        Assert.assertTrue(l0.size() == 7);

        Assert.assertEquals(l0.get(0), "a");
        Assert.assertEquals(l0.get(1), "b");
        Assert.assertEquals(l0.get(6), "g");

        Assert.assertEquals("abcdefg", String.join("", l0.toArray(new String[0])));
        Assert.assertEquals("abcdefk", String.join("", l1.toArray(new String[0])));

        TieredList.Builder<String> t0 = l0.newTier();
        t0.add("h");
        Assert.assertFalse(t0.isSealed());
        t0.seal();
        Assert.assertTrue(t0.isSealed());
        try {
            t0.add("i");
            Assert.fail();
        } catch (Throwable ex) {
        }
        Assert.assertEquals("abcdefgh", String.join("", t0.build().toArray(new String[0])));

        Assert.assertEquals(t0.build().toString(), t0.build().toString(), "TieredList.tier#0\n" +
                "  [0] = a\n" +
                "  [1] = b\n" +
                "  [2] = c\n" +
                "  [3] = d\n" +
                "  [4] = e\n" +
                "  [5] = f\n" +
                "TieredList.tier#1\n" +
                "  [0] = g\n" +
                "TieredList.tier#2\n" +
                "  [0] = h\n");

        TieredList<String> l3 = new ArrayList<>(t0.build()).stream().collect(TieredList.<String>toList());
        Assert.assertEquals(l3.toString(), l3.toString(), "TieredList.tier#0\n" +
                "  [0] = a\n" +
                "  [1] = b\n" +
                "  [2] = c\n" +
                "  [3] = d\n" +
                "  [4] = e\n" +
                "  [5] = f\n" +
                "  [6] = g\n" +
                "  [7] = h\n");

        TieredList<String> l4 = TieredList.<String>genesis().concat(l3.untiered());
        Assert.assertEquals(l4, l3);
    }

    @Test
    public void testTieredMap() {

        TieredMap<String, Integer> m0 = Stream.of(Pair.create("Alice", 23),
                        Pair.create("Bach", 24),
                        Pair.create("Chopin", 25),
                        Pair.create("Vivaldi", 27))
                .collect(TieredMap.toMap(Pair::getFirst, Pair::getSecond));
        Assert.assertTrue(m0.containsKey("Bach"));
        Assert.assertFalse(m0.containsKey("Beethoven"));

        m0 = m0.merge(m0);
        Assert.assertEquals(m0.size(), 4);
        m0 = m0.merge(Collections.emptyMap());
        Assert.assertEquals(m0.size(), 4);

        String csvKeys = m0.keySet().stream().sorted().collect(Collectors.joining(", "));
        Assert.assertEquals(csvKeys, csvKeys, "Alice, Bach, Chopin, Vivaldi");
        String csvValues = m0.values().stream().map(Object::toString).sorted().collect(Collectors.joining(", "));
        Assert.assertEquals(csvValues, csvValues, "23, 24, 25, 27");

        TieredMap.Builder<String, Integer> m1Builder = TieredMap.newGenesisTier();
        m1Builder.put("Mozart", 100);
        m1Builder.put("Schubert", 200);
        TieredMap<String, Integer> m1 = m1Builder.build();
        Assert.assertEquals(m1.toString(), m1.size(), 2);
        Assert.assertEquals(m0.toString(), m0.size(), 4);

        TieredMap<String, Integer> m2 = m1.merge(m0);
        Assert.assertFalse(m2.isEmpty());
        Assert.assertTrue(m2.containsKey("Vivaldi"));
        Assert.assertFalse(m2.containsKey("Handel"));
        TieredMap<String, Integer> m3 =
                m2.entrySet().stream().collect(TieredMap.toMap(e -> e.getKey().toLowerCase(), Map.Entry::getValue));
        String keys = m3.keySet().stream().sorted().collect(Collectors.joining(", "));
        Assert.assertEquals(keys, keys, "alice, bach, chopin, mozart, schubert, vivaldi");
        Assert.assertEquals(m2.get("Mozart").intValue(), 100);
        Assert.assertEquals(m2.get("Chopin").intValue(), 25);
        Assert.assertNull(m2.get("beethoven"));

        TieredMap<String, Integer> m4 = TieredMap.genesis();
        m4 = m4.merge(m3.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
        Assert.assertEquals(m4, m3);
    }

    @Test
    public void testPrettyPrinter() {
        List<Pair<String, Object>> desc = Lists.newArrayList(
                Pair.create("Name", "Mozart"),
                Pair.create("Age", 31),
                Pair.create("Gender", "Man")
        );
        List<PrettyPrinter> items = desc.stream().map(p -> new PrettyPrinter()
                        .addDoubleQuoted(p.getFirst())
                        .spaces(1)
                        .add("=")
                        .spaces(1)
                        .addBacktickQuoted(p.getSecond()))
                .collect(Collectors.toList());
        PrettyPrinter p = new PrettyPrinter();
        p.add("semicolon-before-item:").newLine();
        p.add("[").newLine();
        p.indentEnclose(() -> {
            p.addSuperStepsWithNlDel(";", items);
        });
        p.newLine().add("]");
        p.newLine();
        p.add("semicolon-after-item:").newLine();
        p.add("[").newLine();
        p.indentEnclose(() -> {
            p.addSuperStepsWithDelNl(";", items);
        });

        p.newLine().add("]");
        p.newLine();
        p.add("comma-separated-list:");
        p.add("[").addSuperSteps(", ", items).add("]");

        PrettyPrinter p1 = new PrettyPrinter();
        p1.add("Text BEGIN").newLine();
        p1.indentEnclose(7, () -> {
            p1.addSuperStepWithIndent(p);
        });
        p1.newLine();
        p1.add("Text END");

        String r = p1.getResult();
        Assert.assertEquals(r, r, "Text BEGIN\n" +
                "       semicolon-before-item:\n" +
                "       [\n" +
                "         \"Name\" = `Mozart`\n" +
                "         ;\"Age\" = `31`\n" +
                "         ;\"Gender\" = `Man`\n" +
                "       ]\n" +
                "       semicolon-after-item:\n" +
                "       [\n" +
                "         \"Name\" = `Mozart`;\n" +
                "         \"Age\" = `31`;\n" +
                "         \"Gender\" = `Man`\n" +
                "       ]\n" +
                "       comma-separated-list:[\"Name\" = `Mozart`, \"Age\" = `31`, \"Gender\" = `Man`]\n" +
                "Text END");
    }

    @Test
    public void testPrettyPrintNestedObject() {
        PrettyPrinter p = new PrettyPrinter();
        PrettyPrinter p0 = new PrettyPrinter().addNameToArray("a", Lists.newArrayList(1, 2, 3, 4));
        PrettyPrinter p1 = new PrettyPrinter().addNameToSuperStepArray("b",
                Lists.newArrayList("A", "B", "C", "D").stream()
                        .map(item -> new PrettyPrinter().addDoubleQuoted(item))
                        .collect(Collectors.toList()));
        PrettyPrinter p21 = new PrettyPrinter().addObject(Lists.newArrayList(
                new PrettyPrinter().addNameToObject("abc", new PrettyPrinter().add(1)),
                new PrettyPrinter().addNameToObject("bcd", new PrettyPrinter().add("ABC")),
                new PrettyPrinter().addNameToObject("cde", new PrettyPrinter().add(0.3))
        ));
        PrettyPrinter p2 = new PrettyPrinter().addNameToObject("c", p21);
        p.addObject(Arrays.asList(p0, p1, p2));
        Assert.assertEquals(p.getResult(), "{\n" +
                "  a: [\n" +
                "    1,\n" +
                "    2,\n" +
                "    3,\n" +
                "    4\n" +
                "  ],\n" +
                "  b: [\n" +
                "    \"A\",\n" +
                "    \"B\",\n" +
                "    \"C\",\n" +
                "    \"D\"\n" +
                "  ],\n" +
                "  c: {\n" +
                "    abc: 1,\n" +
                "    bcd: ABC,\n" +
                "    cde: 0.3\n" +
                "  }\n" +
                "}");
    }

    @Test
    public void testPrettyPrintItems() {
        PrettyPrinter p0 = new PrettyPrinter();
        List<PrettyPrinter> printers = Stream.of("a", "b", "c").map(item -> {
            PrettyPrinter p = new PrettyPrinter();
            p.add(item);
            return p;
        }).collect(Collectors.toList());
        p0.add("items=").add("{").newLine();
        p0.indentEnclose(() -> {
            p0.addSuperStepsWithDelNl(",", printers);
        });
        p0.newLine().add("}").newLine();
        Assert.assertEquals(p0.getResult(), "items={\n" +
                "  a,\n" +
                "  b,\n" +
                "  c\n" +
                "}\n");
    }

    @Test
    public void testEscape() {
        String[][] testCases = new String[][] {
                {"\"", "\"\\\"\"", "'\"'"},
                {"\"\\", "\"\\\"\\\\\"", "'\"\\\\'"},
                {"'", "\"'\"", "'\\''"},
                {"'", "\"'\"", "'\\''"},
                {"\n", "\"\\n\"", "'\\n'"},
                {"abc\"def", "\"abc\\\"def\"", "'abc\"def'"},
                {"\"abc\"def", "\"\\\"abc\\\"def\"", "'\"abc\"def'"},
                {"\"abc\"def", "\"\\\"abc\\\"def\"", "'\"abc\"def'"},
                {"\"abc\"def", "\"\\\"abc\\\"def\"", "'\"abc\"def'"},
                {"\"abc\"def", "\"\\\"abc\\\"def\"", "'\"abc\"def'"},
                {"\"abc\"def", "\"\\\"abc\\\"def\"", "'\"abc\"def'"},
                {"\"abc\\\\\n\"def", "\"\\\"abc\\\\\\\\\\n\\\"def\"", "'\"abc\\\\\\\\\\n\"def'"},
        };
        for (String[] tc : testCases) {
            String s = tc[0];
            String expectDoubleQuoted = tc[1];
            String expectSingleQuoted = tc[2];
            Assert.assertEquals(expectDoubleQuoted, PrettyPrinter.escapedDoubleQuoted(s).getResult());
            Assert.assertEquals(expectSingleQuoted, PrettyPrinter.escapedSingleQuoted(s).getResult());
        }
    }

    @Test
    public void testAddSuperSteps() {
        List<PrettyPrinter> printers = com.google.api.client.util.Lists.newArrayList();
        printers.add(new PrettyPrinter().add("a"));
        printers.add(new PrettyPrinter().add("b"));
        printers.add(new PrettyPrinter().add("c"));
        PrettyPrinter p = new PrettyPrinter();
        p.add("[").newLine();
        p.indentEnclose(() -> p.addSuperStepsWithDelNl("#,#", printers));
        p.newLine().add("]");
        Assert.assertEquals(p.getResult(), "[\n" +
                "  a#,#\n" +
                "  b#,#\n" +
                "  c\n" +
                "]");
    }

    @Test
    public void testIdGenerator() {
        {
            Supplier<Integer> idGen = Util.nextIdGenerator();
            String s = IntStream.range(0, 10)
                    .mapToObj(i -> idGen.get())
                    .map(Object::toString)
                    .collect(Collectors.joining("_"));
            Assert.assertEquals(s, s, "0_1_2_3_4_5_6_7_8_9");
        }

        {
            Supplier<Integer> idGen = Util.nextIdGenerator(10);
            String s = IntStream.range(0, 10)
                    .mapToObj(i -> idGen.get())
                    .map(Object::toString)
                    .collect(Collectors.joining("_"));
            Assert.assertEquals(s, s, "10_11_12_13_14_15_16_17_18_19");
        }

        {
            Supplier<Integer> idGen = Util.nextConstGenerator(10);
            String s = IntStream.range(0, 10)
                    .mapToObj(i -> idGen.get())
                    .map(Object::toString)
                    .collect(Collectors.joining("_"));
            Assert.assertEquals(s, s, "10_10_10_10_10_10_10_10_10_10");
        }

        {
            Supplier<Integer> idGen = Util.nextExpGenerator(2, 1);
            String s = IntStream.range(0, 10)
                    .mapToObj(i -> idGen.get())
                    .map(Object::toString)
                    .collect(Collectors.joining("_"));
            Assert.assertEquals(s, s, "2_4_8_16_32_64_128_256_512_1024");
        }
        {
            Supplier<String> idGen = Util.nextStringGenerator("c", "_");
            String s = IntStream.range(0, 10)
                    .mapToObj(i -> idGen.get())
                    .map(Object::toString)
                    .collect(Collectors.joining(","));
            Assert.assertEquals(s, s, "c0_,c1_,c2_,c3_,c4_,c5_,c6_,c7_,c8_,c9_");
        }
        {
            List<Supplier<Integer>> gens = Lists.newArrayList(
                    Util.nextIdGenerator(), Util.nextIdGenerator(), Util.nextIdGenerator());
            Supplier<Optional<List<Integer>>> gen = Util.nextValuesGenerator(4, gens);
            String s = IntStream.range(0, 100)
                    .mapToObj(i -> gen.get())
                    .map(optValues -> optValues.map(
                            values -> values.stream().map(Object::toString).collect(Collectors.joining(""))))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.joining("\n"));

            Assert.assertEquals(s, s, "000\n" +
                    "100\n" +
                    "200\n" +
                    "300\n" +
                    "400\n" +
                    "410\n" +
                    "420\n" +
                    "430\n" +
                    "440\n" +
                    "441\n" +
                    "442\n" +
                    "443\n" +
                    "444");
        }
    }

    @Test
    public void testOnePointIdGenerator() {
        Supplier<Optional<Integer>> idGen = Util.onePoint(1);
        Optional<Integer> a = idGen.get();
        Assert.assertTrue(a.isPresent());
        Assert.assertEquals(a.get().intValue(), 1);
        Assert.assertFalse(idGen.get().isPresent());
    }

    @Test
    public void testWrongHexString() {
        Assert.assertFalse(Util.isHexString("a"));
        Assert.assertFalse(Util.isHexString("akl"));
    }

    @Test
    public void testDowncast() {
        Number a = Integer.valueOf(10);
        Assert.assertTrue(Util.downcast(a, Integer.class).isPresent());
        Assert.assertFalse(Util.downcast(a, String.class).isPresent());
    }

    @Test
    public void testToLong() {
        Assert.assertNull(Util.toLong(null));
        Assert.assertEquals(Util.toLong(100).longValue(), 100L);
        Assert.assertEquals(Util.toLong(100.0).longValue(), 100L);
        Assert.assertEquals(Util.toLong("100").longValue(), 100L);
        Assert.assertNull(Util.toLong("ABC"));
        Assert.assertNull(Util.toLong(new byte[] {'a', 'b', 'c'}));
    }

    @Test
    public void testDigest() {
        String sha1 = Util.sha1("StarRocks");
        Assert.assertEquals(sha1, sha1, "23becf5c8536d5f553e967800b1b80187f7e19da");
        Assert.assertTrue(Util.isHexString(sha1));

        String md5 = Util.md5("StarRocks");
        Assert.assertEquals(md5, md5, "d7bd9d2ff37df58412bd674d7de57e6e");
        Assert.assertTrue(Util.isHexString(md5));
    }

    @Test
    public void testParseDate() {
        String s = Util.yyyyMMddTHHmmss();
        Optional<Date> dt = Util.yyyyMMddTHHmmssToDate(s);
        Assert.assertTrue(dt.isPresent());
        Assert.assertEquals(Util.yyyyMMddTHHmmss(dt.get()), s);
    }
}
