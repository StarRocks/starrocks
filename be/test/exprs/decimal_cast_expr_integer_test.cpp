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

#include <gtest/gtest.h>

#include "exprs/decimal_cast_expr_test_helper.h"
#include "runtime/time_types.h"
#include "types/logical_type.h"

namespace starrocks {

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToBooleanNormal) {
    CastTestCaseArray test_cases = {
            {9, 0, "-985804856", -1, -1, "true", "false"}, {9, 0, "-279848093", -1, -1, "true", "false"},
            {9, 0, "-316053562", -1, -1, "true", "false"}, {9, 0, "-998007094", -1, -1, "true", "false"},
            {9, 0, "-209902734", -1, -1, "true", "false"}, {9, 0, "743730572", -1, -1, "true", "false"},
            {9, 0, "38595", -1, -1, "true", "false"},      {9, 0, "-280834253", -1, -1, "true", "false"},
            {9, 0, "-652846159", -1, -1, "true", "false"}, {9, 0, "31325085", -1, -1, "true", "false"},
            {9, 0, "-378421687", -1, -1, "true", "false"}, {9, 0, "-999796516", -1, -1, "true", "false"},
            {9, 0, "410118519", -1, -1, "true", "false"},  {9, 0, "606678433", -1, -1, "true", "false"},
            {9, 0, "-456494194", -1, -1, "true", "false"}, {9, 0, "-417369874", -1, -1, "true", "false"},
            {9, 0, "710714342", -1, -1, "true", "false"},  {9, 0, "-130877075", -1, -1, "true", "false"},
            {9, 0, "-36888623", -1, -1, "true", "false"},  {9, 0, "-147234325", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{9, 0, "605502594", -1, -1, "", "true"},  {9, 0, "-286752127", -1, -1, "", "true"},
                                    {9, 0, "916921875", -1, -1, "", "true"},  {9, 0, "165337", -1, -1, "", "true"},
                                    {9, 0, "188839695", -1, -1, "", "true"},  {9, 0, "-343239928", -1, -1, "", "true"},
                                    {9, 0, "-999999995", -1, -1, "", "true"}, {9, 0, "86708194", -1, -1, "", "true"},
                                    {9, 0, "790956700", -1, -1, "", "true"},  {9, 0, "-135238034", -1, -1, "", "true"},
                                    {9, 0, "-987731340", -1, -1, "", "true"}, {9, 0, "4958817", -1, -1, "", "true"},
                                    {9, 0, "270040780", -1, -1, "", "true"},  {9, 0, "-869513169", -1, -1, "", "true"},
                                    {9, 0, "700854632", -1, -1, "", "true"},  {9, 0, "-303653335", -1, -1, "", "true"},
                                    {9, 0, "-986889640", -1, -1, "", "true"}, {9, 0, "-537260324", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{9, 0, "2", -1, -1, "2", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{9, 0, "-444785597", -1, -1, "", "true"}, {9, 0, "803800782", -1, -1, "", "true"},
                                    {9, 0, "-397979580", -1, -1, "", "true"}, {9, 0, "765656104", -1, -1, "", "true"},
                                    {9, 0, "1569148", -1, -1, "", "true"},    {9, 0, "-834210141", -1, -1, "", "true"},
                                    {9, 0, "865307293", -1, -1, "", "true"},  {9, 0, "367663708", -1, -1, "", "true"},
                                    {9, 0, "8290897", -1, -1, "", "true"},    {9, 0, "985371505", -1, -1, "", "true"},
                                    {9, 0, "395995574", -1, -1, "", "true"},  {9, 0, "23734479", -1, -1, "", "true"},
                                    {9, 0, "847849081", -1, -1, "", "true"},  {9, 0, "-224764182", -1, -1, "", "true"},
                                    {9, 0, "-695084485", -1, -1, "", "true"}, {9, 0, "-999999686", -1, -1, "", "true"},
                                    {9, 0, "13334885", -1, -1, "", "true"},   {9, 0, "791266589", -1, -1, "", "true"},
                                    {9, 0, "25501868", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL32, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{9, 0, "44", -1, -1, "44", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToIntNormal) {
    CastTestCaseArray test_cases = {{9, 0, "0", -1, -1, "0", "false"},
                                    {9, 0, "201031636", -1, -1, "201031636", "false"},
                                    {9, 0, "143642379", -1, -1, "143642379", "false"},
                                    {9, 0, "-99742994", -1, -1, "-99742994", "false"},
                                    {9, 0, "-161525381", -1, -1, "-161525381", "false"},
                                    {9, 0, "-527884477", -1, -1, "-527884477", "false"},
                                    {9, 0, "-23728162", -1, -1, "-23728162", "false"},
                                    {9, 0, "-999827292", -1, -1, "-999827292", "false"},
                                    {9, 0, "181824805", -1, -1, "181824805", "false"},
                                    {9, 0, "-182536425", -1, -1, "-182536425", "false"},
                                    {9, 0, "479423328", -1, -1, "479423328", "false"},
                                    {9, 0, "-145730297", -1, -1, "-145730297", "false"},
                                    {9, 0, "-280233784", -1, -1, "-280233784", "false"},
                                    {9, 0, "-724772573", -1, -1, "-724772573", "false"},
                                    {9, 0, "780052698", -1, -1, "780052698", "false"},
                                    {9, 0, "-170771184", -1, -1, "-170771184", "false"},
                                    {9, 0, "-742874285", -1, -1, "-742874285", "false"},
                                    {9, 0, "734784312", -1, -1, "734784312", "false"},
                                    {9, 0, "-662912909", -1, -1, "-662912909", "false"},
                                    {9, 0, "20418", -1, -1, "20418", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToBigIntNormal) {
    CastTestCaseArray test_cases = {{9, 0, "-245517624", -1, -1, "-245517624", "false"},
                                    {9, 0, "239", -1, -1, "239", "false"},
                                    {9, 0, "267794", -1, -1, "267794", "false"},
                                    {9, 0, "359062338", -1, -1, "359062338", "false"},
                                    {9, 0, "424", -1, -1, "424", "false"},
                                    {9, 0, "-43527857", -1, -1, "-43527857", "false"},
                                    {9, 0, "-247685988", -1, -1, "-247685988", "false"},
                                    {9, 0, "-668812539", -1, -1, "-668812539", "false"},
                                    {9, 0, "-999990792", -1, -1, "-999990792", "false"},
                                    {9, 0, "699905114", -1, -1, "699905114", "false"},
                                    {9, 0, "692411587", -1, -1, "692411587", "false"},
                                    {9, 0, "453431238", -1, -1, "453431238", "false"},
                                    {9, 0, "-216302086", -1, -1, "-216302086", "false"},
                                    {9, 0, "160459274", -1, -1, "160459274", "false"},
                                    {9, 0, "134100115", -1, -1, "134100115", "false"},
                                    {9, 0, "216485521", -1, -1, "216485521", "false"},
                                    {9, 0, "94", -1, -1, "94", "false"},
                                    {9, 0, "347", -1, -1, "347", "false"},
                                    {9, 0, "-721429130", -1, -1, "-721429130", "false"},
                                    {9, 0, "-202799719", -1, -1, "-202799719", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s0ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{9, 0, "-999999028", -1, -1, "-999999028", "false"},
                                    {9, 0, "-425320602", -1, -1, "-425320602", "false"},
                                    {9, 0, "-952085399", -1, -1, "-952085399", "false"},
                                    {9, 0, "-743792282", -1, -1, "-743792282", "false"},
                                    {9, 0, "5", -1, -1, "5", "false"},
                                    {9, 0, "245543337", -1, -1, "245543337", "false"},
                                    {9, 0, "-138520280", -1, -1, "-138520280", "false"},
                                    {9, 0, "728434024", -1, -1, "728434024", "false"},
                                    {9, 0, "-381366246", -1, -1, "-381366246", "false"},
                                    {9, 0, "727001043", -1, -1, "727001043", "false"},
                                    {9, 0, "968427941", -1, -1, "968427941", "false"},
                                    {9, 0, "-126748932", -1, -1, "-126748932", "false"},
                                    {9, 0, "-572029588", -1, -1, "-572029588", "false"},
                                    {9, 0, "867465395", -1, -1, "867465395", "false"},
                                    {9, 0, "987011855", -1, -1, "987011855", "false"},
                                    {9, 0, "621040643", -1, -1, "621040643", "false"},
                                    {9, 0, "466389", -1, -1, "466389", "false"},
                                    {9, 0, "-949310806", -1, -1, "-949310806", "false"},
                                    {9, 0, "942656219", -1, -1, "942656219", "false"},
                                    {9, 0, "-206745765", -1, -1, "-206745765", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToBooleanNormal) {
    CastTestCaseArray test_cases = {
            {9, 2, "-6996368.05", -1, -1, "true", "false"}, {9, 2, "9194936.38", -1, -1, "true", "false"},
            {9, 2, "378950.39", -1, -1, "true", "false"},   {9, 2, "7920953.36", -1, -1, "true", "false"},
            {9, 2, "9831581.96", -1, -1, "true", "false"},  {9, 2, "-9999895.36", -1, -1, "true", "false"},
            {9, 2, "7578967.94", -1, -1, "true", "false"},  {9, 2, "-8284741.22", -1, -1, "true", "false"},
            {9, 2, "-3021656.86", -1, -1, "true", "false"}, {9, 2, "8201507.54", -1, -1, "true", "false"},
            {9, 2, "1563522.15", -1, -1, "true", "false"},  {9, 2, "-8271813.56", -1, -1, "true", "false"},
            {9, 2, "-24152.73", -1, -1, "true", "false"},   {9, 2, "-4905653.03", -1, -1, "true", "false"},
            {9, 2, "7684582.00", -1, -1, "true", "false"},  {9, 2, "-9366485.18", -1, -1, "true", "false"},
            {9, 2, "9327378.97", -1, -1, "true", "false"},  {9, 2, "-2978132.89", -1, -1, "true", "false"},
            {9, 2, "-3097244.07", -1, -1, "true", "false"}, {9, 2, "-6501238.16", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {
            {9, 2, "-9999999.96", -1, -1, "", "true"}, {9, 2, "-9999869.23", -1, -1, "", "true"},
            {9, 2, "9543951.39", -1, -1, "", "true"},  {9, 2, "167451.16", -1, -1, "", "true"},
            {9, 2, "-9999938.15", -1, -1, "", "true"}, {9, 2, "6820910.09", -1, -1, "", "true"},
            {9, 2, "8025075.83", -1, -1, "", "true"},  {9, 2, "-2902122.50", -1, -1, "", "true"},
            {9, 2, "-9999977.59", -1, -1, "", "true"}, {9, 2, "5487245.22", -1, -1, "", "true"},
            {9, 2, "2337685.25", -1, -1, "", "true"},  {9, 2, "-4856017.86", -1, -1, "", "true"},
            {9, 2, "-3047630.21", -1, -1, "", "true"}, {9, 2, "2324891.90", -1, -1, "", "true"},
            {9, 2, "-9999703.86", -1, -1, "", "true"}, {9, 2, "-3082828.75", -1, -1, "", "true"},
            {9, 2, "5895753.75", -1, -1, "", "true"},  {9, 2, "7269177.53", -1, -1, "", "true"},
            {9, 2, "-2797168.51", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{9, 2, "49.30", -1, -1, "49", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {
            {9, 2, "5271855.79", -1, -1, "", "true"},  {9, 2, "8393354.60", -1, -1, "", "true"},
            {9, 2, "-2536759.98", -1, -1, "", "true"}, {9, 2, "4342984.36", -1, -1, "", "true"},
            {9, 2, "-7635929.14", -1, -1, "", "true"}, {9, 2, "133710.86", -1, -1, "", "true"},
            {9, 2, "9889781.08", -1, -1, "", "true"},  {9, 2, "-2104253.12", -1, -1, "", "true"},
            {9, 2, "1828937.19", -1, -1, "", "true"},  {9, 2, "-6348465.26", -1, -1, "", "true"},
            {9, 2, "-6274644.10", -1, -1, "", "true"}, {9, 2, "-9912053.34", -1, -1, "", "true"},
            {9, 2, "-6283246.21", -1, -1, "", "true"}, {9, 2, "8273417.49", -1, -1, "", "true"},
            {9, 2, "5304866.08", -1, -1, "", "true"},  {9, 2, "9239850.68", -1, -1, "", "true"},
            {9, 2, "-2382799.51", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL32, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{9, 2, "8268.79", -1, -1, "8268", "false"},
                                    {9, 2, "5.84", -1, -1, "5", "false"},
                                    {9, 2, "0.13", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 2, "9540600.69", -1, -1, "9540600", "false"},   {9, 2, "5217014.32", -1, -1, "5217014", "false"},
            {9, 2, "8805969.31", -1, -1, "8805969", "false"},   {9, 2, "-9952636.37", -1, -1, "-9952636", "false"},
            {9, 2, "-8722420.49", -1, -1, "-8722420", "false"}, {9, 2, "-9999968.77", -1, -1, "-9999968", "false"},
            {9, 2, "41595.69", -1, -1, "41595", "false"},       {9, 2, "-9998384.62", -1, -1, "-9998384", "false"},
            {9, 2, "-8889426.65", -1, -1, "-8889426", "false"}, {9, 2, "-8349801.77", -1, -1, "-8349801", "false"},
            {9, 2, "35613.89", -1, -1, "35613", "false"},       {9, 2, "-5477575.66", -1, -1, "-5477575", "false"},
            {9, 2, "148707.73", -1, -1, "148707", "false"},     {9, 2, "-9999999.70", -1, -1, "-9999999", "false"},
            {9, 2, "1818248.05", -1, -1, "1818248", "false"},   {9, 2, "-3621921.58", -1, -1, "-3621921", "false"},
            {9, 2, "4930803.05", -1, -1, "4930803", "false"},   {9, 2, "0.51", -1, -1, "0", "false"},
            {9, 2, "6110687.38", -1, -1, "6110687", "false"},   {9, 2, "5435053.40", -1, -1, "5435053", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToBigIntNormal) {
    CastTestCaseArray test_cases = {{9, 2, "9499468.54", -1, -1, "9499468", "false"},
                                    {9, 2, "6916704.07", -1, -1, "6916704", "false"},
                                    {9, 2, "6790006.23", -1, -1, "6790006", "false"},
                                    {9, 2, "3887414.99", -1, -1, "3887414", "false"},
                                    {9, 2, "4096961.55", -1, -1, "4096961", "false"},
                                    {9, 2, "-654349.01", -1, -1, "-654349", "false"},
                                    {9, 2, "-1871049.25", -1, -1, "-1871049", "false"},
                                    {9, 2, "2211122.47", -1, -1, "2211122", "false"},
                                    {9, 2, "-9999513.64", -1, -1, "-9999513", "false"},
                                    {9, 2, "0.19", -1, -1, "0", "false"},
                                    {9, 2, "-9129025.30", -1, -1, "-9129025", "false"},
                                    {9, 2, "7260821.12", -1, -1, "7260821", "false"},
                                    {9, 2, "-3159644.91", -1, -1, "-3159644", "false"},
                                    {9, 2, "7853468.67", -1, -1, "7853468", "false"},
                                    {9, 2, "1.11", -1, -1, "1", "false"},
                                    {9, 2, "-901826.33", -1, -1, "-901826", "false"},
                                    {9, 2, "-7233714.36", -1, -1, "-7233714", "false"},
                                    {9, 2, "-5248939.58", -1, -1, "-5248939", "false"},
                                    {9, 2, "-8823010.19", -1, -1, "-8823010", "false"},
                                    {9, 2, "-9835488.55", -1, -1, "-9835488", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s2ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 2, "-2300509.48", -1, -1, "-2300509", "false"}, {9, 2, "9212897.15", -1, -1, "9212897", "false"},
            {9, 2, "8026575.38", -1, -1, "8026575", "false"},   {9, 2, "3836293.82", -1, -1, "3836293", "false"},
            {9, 2, "-7625737.12", -1, -1, "-7625737", "false"}, {9, 2, "-6703044.87", -1, -1, "-6703044", "false"},
            {9, 2, "-9953527.62", -1, -1, "-9953527", "false"}, {9, 2, "-9999994.90", -1, -1, "-9999994", "false"},
            {9, 2, "1466475.86", -1, -1, "1466475", "false"},   {9, 2, "7400046.20", -1, -1, "7400046", "false"},
            {9, 2, "8721373.08", -1, -1, "8721373", "false"},   {9, 2, "-6646582.70", -1, -1, "-6646582", "false"},
            {9, 2, "-1757463.53", -1, -1, "-1757463", "false"}, {9, 2, "8490563.19", -1, -1, "8490563", "false"},
            {9, 2, "-2960865.24", -1, -1, "-2960865", "false"}, {9, 2, "-9456746.89", -1, -1, "-9456746", "false"},
            {9, 2, "2828751.29", -1, -1, "2828751", "false"},   {9, 2, "-5475012.17", -1, -1, "-5475012", "false"},
            {9, 2, "-7260308.46", -1, -1, "-7260308", "false"}, {9, 2, "9810297.63", -1, -1, "9810297", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s9ToBooleanNormal) {
    CastTestCaseArray test_cases = {
            {9, 9, "0.856735011", -1, -1, "true", "false"},  {9, 9, "0.037521458", -1, -1, "true", "false"},
            {9, 9, "0.123259717", -1, -1, "true", "false"},  {9, 9, "0.000001751", -1, -1, "true", "false"},
            {9, 9, "0.499341822", -1, -1, "true", "false"},  {9, 9, "-0.807309134", -1, -1, "true", "false"},
            {9, 9, "0.270666369", -1, -1, "true", "false"},  {9, 9, "0.495705903", -1, -1, "true", "false"},
            {9, 9, "-0.432702244", -1, -1, "true", "false"}, {9, 9, "0.910866610", -1, -1, "true", "false"},
            {9, 9, "-0.896451271", -1, -1, "true", "false"}, {9, 9, "0.631280554", -1, -1, "true", "false"},
            {9, 9, "-0.391679518", -1, -1, "true", "false"}, {9, 9, "1.4E-8", -1, -1, "true", "false"},
            {9, 9, "0.197361117", -1, -1, "true", "false"},  {9, 9, "-0.956948392", -1, -1, "true", "false"},
            {9, 9, "0.386645960", -1, -1, "true", "false"},  {9, 9, "-0.181034168", -1, -1, "true", "false"},
            {9, 9, "0.818442399", -1, -1, "true", "false"},  {9, 9, "-0.150514334", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s9ToTinyIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 9, "0.542833075", -1, -1, "0", "false"},  {9, 9, "-0.759897375", -1, -1, "0", "false"},
            {9, 9, "-0.063431350", -1, -1, "0", "false"}, {9, 9, "0.524316060", -1, -1, "0", "false"},
            {9, 9, "0.930750165", -1, -1, "0", "false"},  {9, 9, "2.4E-8", -1, -1, "0", "false"},
            {9, 9, "0.971835455", -1, -1, "0", "false"},  {9, 9, "-0.999999901", -1, -1, "0", "false"},
            {9, 9, "0.901589979", -1, -1, "0", "false"},  {9, 9, "0.902712506", -1, -1, "0", "false"},
            {9, 9, "-0.678217426", -1, -1, "0", "false"}, {9, 9, "-0.993434356", -1, -1, "0", "false"},
            {9, 9, "0.132057792", -1, -1, "0", "false"},  {9, 9, "-0.302145565", -1, -1, "0", "false"},
            {9, 9, "-0.353542108", -1, -1, "0", "false"}, {9, 9, "0.283254692", -1, -1, "0", "false"},
            {9, 9, "-0.226949163", -1, -1, "0", "false"}, {9, 9, "0.272694258", -1, -1, "0", "false"},
            {9, 9, "0.930030040", -1, -1, "0", "false"},  {9, 9, "-0.062081998", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s9ToSmallIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 9, "-0.256434586", -1, -1, "0", "false"}, {9, 9, "0.206320258", -1, -1, "0", "false"},
            {9, 9, "-0.113726072", -1, -1, "0", "false"}, {9, 9, "-0.983423899", -1, -1, "0", "false"},
            {9, 9, "-0.480013336", -1, -1, "0", "false"}, {9, 9, "0.748616126", -1, -1, "0", "false"},
            {9, 9, "-0.999999968", -1, -1, "0", "false"}, {9, 9, "-0.995579418", -1, -1, "0", "false"},
            {9, 9, "-0.454360505", -1, -1, "0", "false"}, {9, 9, "-0.681325830", -1, -1, "0", "false"},
            {9, 9, "0.115036379", -1, -1, "0", "false"},  {9, 9, "-0.548005841", -1, -1, "0", "false"},
            {9, 9, "-0.495519877", -1, -1, "0", "false"}, {9, 9, "-0.757364973", -1, -1, "0", "false"},
            {9, 9, "-0.386817084", -1, -1, "0", "false"}, {9, 9, "0.177073422", -1, -1, "0", "false"},
            {9, 9, "0.838235948", -1, -1, "0", "false"},  {9, 9, "-0.469479220", -1, -1, "0", "false"},
            {9, 9, "0.000179280", -1, -1, "0", "false"},  {9, 9, "0.000487577", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s9ToIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 9, "-0.283667068", -1, -1, "0", "false"}, {9, 9, "0.858341926", -1, -1, "0", "false"},
            {9, 9, "-0.389941726", -1, -1, "0", "false"}, {9, 9, "-0.999834200", -1, -1, "0", "false"},
            {9, 9, "0.598963507", -1, -1, "0", "false"},  {9, 9, "0.677706642", -1, -1, "0", "false"},
            {9, 9, "-0.534365551", -1, -1, "0", "false"}, {9, 9, "0.805398809", -1, -1, "0", "false"},
            {9, 9, "-0.466526492", -1, -1, "0", "false"}, {9, 9, "-0.426063952", -1, -1, "0", "false"},
            {9, 9, "8.31E-7", -1, -1, "0", "false"},      {9, 9, "-0.999999967", -1, -1, "0", "false"},
            {9, 9, "0.206283471", -1, -1, "0", "false"},  {9, 9, "-0.263230528", -1, -1, "0", "false"},
            {9, 9, "0.881796112", -1, -1, "0", "false"},  {9, 9, "0.874550043", -1, -1, "0", "false"},
            {9, 9, "-0.069849034", -1, -1, "0", "false"}, {9, 9, "-0.135822185", -1, -1, "0", "false"},
            {9, 9, "0.168330090", -1, -1, "0", "false"},  {9, 9, "-0.247037283", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s9ToBigIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 9, "0.976290635", -1, -1, "0", "false"},  {9, 9, "-0.329108432", -1, -1, "0", "false"},
            {9, 9, "-0.496323529", -1, -1, "0", "false"}, {9, 9, "-0.999999951", -1, -1, "0", "false"},
            {9, 9, "0.865560279", -1, -1, "0", "false"},  {9, 9, "5.4E-8", -1, -1, "0", "false"},
            {9, 9, "0.000008996", -1, -1, "0", "false"},  {9, 9, "-0.350407465", -1, -1, "0", "false"},
            {9, 9, "0.723959816", -1, -1, "0", "false"},  {9, 9, "0.936325255", -1, -1, "0", "false"},
            {9, 9, "-0.813135564", -1, -1, "0", "false"}, {9, 9, "-0.879623753", -1, -1, "0", "false"},
            {9, 9, "-0.360989030", -1, -1, "0", "false"}, {9, 9, "0.851291700", -1, -1, "0", "false"},
            {9, 9, "-0.873563998", -1, -1, "0", "false"}, {9, 9, "3E-9", -1, -1, "0", "false"},
            {9, 9, "0.238852803", -1, -1, "0", "false"},  {9, 9, "-0.957965593", -1, -1, "0", "false"},
            {9, 9, "-0.723830950", -1, -1, "0", "false"}, {9, 9, "0.466414973", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p9s9ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {9, 9, "0.547980912", -1, -1, "0", "false"},  {9, 9, "0.645946212", -1, -1, "0", "false"},
            {9, 9, "0.005159663", -1, -1, "0", "false"},  {9, 9, "0.218109165", -1, -1, "0", "false"},
            {9, 9, "-0.257723112", -1, -1, "0", "false"}, {9, 9, "0.023953515", -1, -1, "0", "false"},
            {9, 9, "0.615215977", -1, -1, "0", "false"},  {9, 9, "-0.262943596", -1, -1, "0", "false"},
            {9, 9, "-0.999996379", -1, -1, "0", "false"}, {9, 9, "0.306789939", -1, -1, "0", "false"},
            {9, 9, "0.177535284", -1, -1, "0", "false"},  {9, 9, "-0.705127305", -1, -1, "0", "false"},
            {9, 9, "-0.269382401", -1, -1, "0", "false"}, {9, 9, "0.205821741", -1, -1, "0", "false"},
            {9, 9, "0.042447361", -1, -1, "0", "false"},  {9, 9, "-0.298339488", -1, -1, "0", "false"},
            {9, 9, "-0.437323399", -1, -1, "0", "false"}, {9, 9, "-0.724447054", -1, -1, "0", "false"},
            {9, 9, "-0.280508502", -1, -1, "0", "false"}, {9, 9, "-0.387298092", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToBooleanNormal) {
    CastTestCaseArray test_cases = {
            {7, 4, "-373.3128", -1, -1, "true", "false"}, {7, 4, "-156.8444", -1, -1, "true", "false"},
            {7, 4, "0.0065", -1, -1, "true", "false"},    {7, 4, "-140.7690", -1, -1, "true", "false"},
            {7, 4, "-35.7918", -1, -1, "true", "false"},  {7, 4, "-113.4896", -1, -1, "true", "false"},
            {7, 4, "-997.0549", -1, -1, "true", "false"}, {7, 4, "452.5588", -1, -1, "true", "false"},
            {7, 4, "-596.9932", -1, -1, "true", "false"}, {7, 4, "427.2139", -1, -1, "true", "false"},
            {7, 4, "-765.4473", -1, -1, "true", "false"}, {7, 4, "-212.4464", -1, -1, "true", "false"},
            {7, 4, "-101.6530", -1, -1, "true", "false"}, {7, 4, "-52.1162", -1, -1, "true", "false"},
            {7, 4, "-998.4658", -1, -1, "true", "false"}, {7, 4, "387.5558", -1, -1, "true", "false"},
            {7, 4, "-948.2423", -1, -1, "true", "false"}, {7, 4, "-383.2409", -1, -1, "true", "false"},
            {7, 4, "-628.8595", -1, -1, "true", "false"}, {7, 4, "644.9703", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{7, 4, "-361.6531", -1, -1, "", "true"}, {7, 4, "141.7919", -1, -1, "", "true"},
                                    {7, 4, "776.5858", -1, -1, "", "true"},  {7, 4, "842.7347", -1, -1, "", "true"},
                                    {7, 4, "169.1737", -1, -1, "", "true"},  {7, 4, "-969.4388", -1, -1, "", "true"},
                                    {7, 4, "624.5131", -1, -1, "", "true"},  {7, 4, "-791.3865", -1, -1, "", "true"},
                                    {7, 4, "-397.8369", -1, -1, "", "true"}, {7, 4, "-590.5079", -1, -1, "", "true"},
                                    {7, 4, "-238.7716", -1, -1, "", "true"}, {7, 4, "-694.2378", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{7, 4, "18.9813", -1, -1, "18", "false"}, {7, 4, "83.7989", -1, -1, "83", "false"},
                                    {7, 4, "99.9698", -1, -1, "99", "false"}, {7, 4, "0.0004", -1, -1, "0", "false"},
                                    {7, 4, "2.4922", -1, -1, "2", "false"},   {7, 4, "23.8213", -1, -1, "23", "false"},
                                    {7, 4, "0.0007", -1, -1, "0", "false"},   {7, 4, "74.8839", -1, -1, "74", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToSmallIntNormal) {
    CastTestCaseArray test_cases = {
            {7, 4, "-403.2898", -1, -1, "-403", "false"}, {7, 4, "779.2956", -1, -1, "779", "false"},
            {7, 4, "-11.0349", -1, -1, "-11", "false"},   {7, 4, "0.7159", -1, -1, "0", "false"},
            {7, 4, "587.0521", -1, -1, "587", "false"},   {7, 4, "44.5318", -1, -1, "44", "false"},
            {7, 4, "382.2575", -1, -1, "382", "false"},   {7, 4, "-936.4111", -1, -1, "-936", "false"},
            {7, 4, "178.8233", -1, -1, "178", "false"},   {7, 4, "46.2569", -1, -1, "46", "false"},
            {7, 4, "-22.8100", -1, -1, "-22", "false"},   {7, 4, "-824.4335", -1, -1, "-824", "false"},
            {7, 4, "903.9880", -1, -1, "903", "false"},   {7, 4, "689.3492", -1, -1, "689", "false"},
            {7, 4, "984.6129", -1, -1, "984", "false"},   {7, 4, "-211.0037", -1, -1, "-211", "false"},
            {7, 4, "133.9386", -1, -1, "133", "false"},   {7, 4, "206.0609", -1, -1, "206", "false"},
            {7, 4, "363.3118", -1, -1, "363", "false"},   {7, 4, "-684.4928", -1, -1, "-684", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToIntNormal) {
    CastTestCaseArray test_cases = {
            {7, 4, "-401.4228", -1, -1, "-401", "false"}, {7, 4, "600.5355", -1, -1, "600", "false"},
            {7, 4, "-410.9161", -1, -1, "-410", "false"}, {7, 4, "-671.4844", -1, -1, "-671", "false"},
            {7, 4, "534.0823", -1, -1, "534", "false"},   {7, 4, "-73.7607", -1, -1, "-73", "false"},
            {7, 4, "-999.9743", -1, -1, "-999", "false"}, {7, 4, "-999.9938", -1, -1, "-999", "false"},
            {7, 4, "0.0026", -1, -1, "0", "false"},       {7, 4, "19.5661", -1, -1, "19", "false"},
            {7, 4, "-864.5397", -1, -1, "-864", "false"}, {7, 4, "-914.8199", -1, -1, "-914", "false"},
            {7, 4, "776.4096", -1, -1, "776", "false"},   {7, 4, "287.5799", -1, -1, "287", "false"},
            {7, 4, "-999.6644", -1, -1, "-999", "false"}, {7, 4, "-640.4108", -1, -1, "-640", "false"},
            {7, 4, "-310.0009", -1, -1, "-310", "false"}, {7, 4, "398.1787", -1, -1, "398", "false"},
            {7, 4, "-299.6889", -1, -1, "-299", "false"}, {7, 4, "77.4635", -1, -1, "77", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToBigIntNormal) {
    CastTestCaseArray test_cases = {
            {7, 4, "377.4617", -1, -1, "377", "false"},   {7, 4, "638.3467", -1, -1, "638", "false"},
            {7, 4, "-999.9983", -1, -1, "-999", "false"}, {7, 4, "-504.0138", -1, -1, "-504", "false"},
            {7, 4, "-999.9994", -1, -1, "-999", "false"}, {7, 4, "-716.8498", -1, -1, "-716", "false"},
            {7, 4, "178.3866", -1, -1, "178", "false"},   {7, 4, "-652.9884", -1, -1, "-652", "false"},
            {7, 4, "-908.5891", -1, -1, "-908", "false"}, {7, 4, "-906.6460", -1, -1, "-906", "false"},
            {7, 4, "557.4183", -1, -1, "557", "false"},   {7, 4, "452.7416", -1, -1, "452", "false"},
            {7, 4, "706.1175", -1, -1, "706", "false"},   {7, 4, "-544.9078", -1, -1, "-544", "false"},
            {7, 4, "464.6760", -1, -1, "464", "false"},   {7, 4, "-414.9267", -1, -1, "-414", "false"},
            {7, 4, "0.7688", -1, -1, "0", "false"},       {7, 4, "642.4047", -1, -1, "642", "false"},
            {7, 4, "-655.2350", -1, -1, "-655", "false"}, {7, 4, "235.0891", -1, -1, "235", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal32p7s4ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {7, 4, "386.8108", -1, -1, "386", "false"},   {7, 4, "0.1342", -1, -1, "0", "false"},
            {7, 4, "0.0008", -1, -1, "0", "false"},       {7, 4, "-750.1137", -1, -1, "-750", "false"},
            {7, 4, "-912.7679", -1, -1, "-912", "false"}, {7, 4, "0.0000", -1, -1, "0", "false"},
            {7, 4, "-829.6059", -1, -1, "-829", "false"}, {7, 4, "826.7481", -1, -1, "826", "false"},
            {7, 4, "0.0001", -1, -1, "0", "false"},       {7, 4, "116.6333", -1, -1, "116", "false"},
            {7, 4, "599.7987", -1, -1, "599", "false"},   {7, 4, "-642.8497", -1, -1, "-642", "false"},
            {7, 4, "-957.9325", -1, -1, "-957", "false"}, {7, 4, "-64.0118", -1, -1, "-64", "false"},
            {7, 4, "956.9464", -1, -1, "956", "false"},   {7, 4, "536.1421", -1, -1, "536", "false"},
            {7, 4, "-329.0420", -1, -1, "-329", "false"}, {7, 4, "113.1013", -1, -1, "113", "false"},
            {7, 4, "-199.6264", -1, -1, "-199", "false"}, {7, 4, "820.6298", -1, -1, "820", "false"}};
    test_cast_all<TYPE_DECIMAL32, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToBooleanNormal) {
    CastTestCaseArray test_cases = {{18, 0, "-988229915042023373", -1, -1, "true", "false"},
                                    {18, 0, "-675435532689367177", -1, -1, "true", "false"},
                                    {18, 0, "88265780184675283", -1, -1, "true", "false"},
                                    {18, 0, "-777689350498369008", -1, -1, "true", "false"},
                                    {18, 0, "-696043183912223612", -1, -1, "true", "false"},
                                    {18, 0, "-982712446186593419", -1, -1, "true", "false"},
                                    {18, 0, "-185600660072220437", -1, -1, "true", "false"},
                                    {18, 0, "-543888253546172340", -1, -1, "true", "false"},
                                    {18, 0, "-999999999967230653", -1, -1, "true", "false"},
                                    {18, 0, "-999999998316547259", -1, -1, "true", "false"},
                                    {18, 0, "797857985192063312", -1, -1, "true", "false"},
                                    {18, 0, "-999999999977883948", -1, -1, "true", "false"},
                                    {18, 0, "-490149576849308776", -1, -1, "true", "false"},
                                    {18, 0, "-999999934537994473", -1, -1, "true", "false"},
                                    {18, 0, "9", -1, -1, "true", "false"},
                                    {18, 0, "-999999999977148690", -1, -1, "true", "false"},
                                    {18, 0, "435157410329326465", -1, -1, "true", "false"},
                                    {18, 0, "-999999999936323567", -1, -1, "true", "false"},
                                    {18, 0, "-880901946663840558", -1, -1, "true", "false"},
                                    {18, 0, "-549532198295160271", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{18, 0, "3723867794", -1, -1, "", "true"},
                                    {18, 0, "1213", -1, -1, "", "true"},
                                    {18, 0, "-414760927446278438", -1, -1, "", "true"},
                                    {18, 0, "716534", -1, -1, "", "true"},
                                    {18, 0, "10021891", -1, -1, "", "true"},
                                    {18, 0, "-999993425453956997", -1, -1, "", "true"},
                                    {18, 0, "-999999999999999791", -1, -1, "", "true"},
                                    {18, 0, "921560355282329901", -1, -1, "", "true"},
                                    {18, 0, "-624678717621018960", -1, -1, "", "true"},
                                    {18, 0, "-999999999999855309", -1, -1, "", "true"},
                                    {18, 0, "949524069263293053", -1, -1, "", "true"},
                                    {18, 0, "15314360", -1, -1, "", "true"},
                                    {18, 0, "496037792784273263", -1, -1, "", "true"},
                                    {18, 0, "-639450062651030724", -1, -1, "", "true"},
                                    {18, 0, "691806645767367297", -1, -1, "", "true"},
                                    {18, 0, "6571394287277465", -1, -1, "", "true"},
                                    {18, 0, "-505115889213866844", -1, -1, "", "true"},
                                    {18, 0, "-999999999999413121", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{18, 0, "31", -1, -1, "31", "false"}, {18, 0, "1", -1, -1, "1", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {
            {18, 0, "-231736280331810614", -1, -1, "", "true"}, {18, 0, "-587904943241214089", -1, -1, "", "true"},
            {18, 0, "171639545", -1, -1, "", "true"},           {18, 0, "52679", -1, -1, "", "true"},
            {18, 0, "28059708736269604", -1, -1, "", "true"},   {18, 0, "-220051077354962884", -1, -1, "", "true"},
            {18, 0, "-619368028644957175", -1, -1, "", "true"}, {18, 0, "738940286430260517", -1, -1, "", "true"},
            {18, 0, "-999992352823989764", -1, -1, "", "true"}, {18, 0, "902806197977256567", -1, -1, "", "true"},
            {18, 0, "419525842435156138", -1, -1, "", "true"},  {18, 0, "-921831813305230202", -1, -1, "", "true"},
            {18, 0, "618736366608962582", -1, -1, "", "true"},  {18, 0, "-999342950784478890", -1, -1, "", "true"},
            {18, 0, "-661259271567046738", -1, -1, "", "true"}, {18, 0, "929366807562186", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{18, 0, "777", -1, -1, "777", "false"},
                                    {18, 0, "4838", -1, -1, "4838", "false"},
                                    {18, 0, "2", -1, -1, "2", "false"},
                                    {18, 0, "8", -1, -1, "8", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToIntAbnormal) {
    CastTestCaseArray test_cases = {
            {18, 0, "-999999999999999744", -1, -1, "", "true"}, {18, 0, "-998425842255706432", -1, -1, "", "true"},
            {18, 0, "798050935571001088", -1, -1, "", "true"},  {18, 0, "33604480863", -1, -1, "", "true"},
            {18, 0, "172363055677471695", -1, -1, "", "true"},  {18, 0, "-999999999614815936", -1, -1, "", "true"},
            {18, 0, "713056924584170250", -1, -1, "", "true"},  {18, 0, "-210724243843906334", -1, -1, "", "true"},
            {18, 0, "582086486027967209", -1, -1, "", "true"},  {18, 0, "-963211802275179154", -1, -1, "", "true"},
            {18, 0, "99146631046074904", -1, -1, "", "true"},   {18, 0, "-315210476306765082", -1, -1, "", "true"},
            {18, 0, "-999999610900336578", -1, -1, "", "true"}, {18, 0, "570209531076431270", -1, -1, "", "true"},
            {18, 0, "17218570820", -1, -1, "", "true"},         {18, 0, "-877673631054136367", -1, -1, "", "true"},
            {18, 0, "-990823118796815853", -1, -1, "", "true"}, {18, 0, "-109753853845815168", -1, -1, "", "true"},
            {18, 0, "-982343478834289187", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToIntNormal) {
    CastTestCaseArray test_cases = {{18, 0, "1924164", -1, -1, "1924164", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToBigIntNormal) {
    CastTestCaseArray test_cases = {{18, 0, "-53097239902346494", -1, -1, "-53097239902346494", "false"},
                                    {18, 0, "-166777519475561368", -1, -1, "-166777519475561368", "false"},
                                    {18, 0, "766617669743413233", -1, -1, "766617669743413233", "false"},
                                    {18, 0, "23", -1, -1, "23", "false"},
                                    {18, 0, "-256137091823267305", -1, -1, "-256137091823267305", "false"},
                                    {18, 0, "117913239892045826", -1, -1, "117913239892045826", "false"},
                                    {18, 0, "447979297880330700", -1, -1, "447979297880330700", "false"},
                                    {18, 0, "357882844754176861", -1, -1, "357882844754176861", "false"},
                                    {18, 0, "42581094450204195", -1, -1, "42581094450204195", "false"},
                                    {18, 0, "-999999868104132807", -1, -1, "-999999868104132807", "false"},
                                    {18, 0, "48258378080453269", -1, -1, "48258378080453269", "false"},
                                    {18, 0, "-631489721225801962", -1, -1, "-631489721225801962", "false"},
                                    {18, 0, "-999999999999991496", -1, -1, "-999999999999991496", "false"},
                                    {18, 0, "861068217052645130", -1, -1, "861068217052645130", "false"},
                                    {18, 0, "-632434658684306169", -1, -1, "-632434658684306169", "false"},
                                    {18, 0, "26", -1, -1, "26", "false"},
                                    {18, 0, "207493550001555", -1, -1, "207493550001555", "false"},
                                    {18, 0, "-766361314653915870", -1, -1, "-766361314653915870", "false"},
                                    {18, 0, "591653346268714659", -1, -1, "591653346268714659", "false"},
                                    {18, 0, "-424060967858621460", -1, -1, "-424060967858621460", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s0ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{18, 0, "-893163793819733878", -1, -1, "-893163793819733878", "false"},
                                    {18, 0, "-257749028213815725", -1, -1, "-257749028213815725", "false"},
                                    {18, 0, "-776995459290203078", -1, -1, "-776995459290203078", "false"},
                                    {18, 0, "55", -1, -1, "55", "false"},
                                    {18, 0, "-942576927108651666", -1, -1, "-942576927108651666", "false"},
                                    {18, 0, "120039741576629293", -1, -1, "120039741576629293", "false"},
                                    {18, 0, "130492714216603987", -1, -1, "130492714216603987", "false"},
                                    {18, 0, "-999999971355725042", -1, -1, "-999999971355725042", "false"},
                                    {18, 0, "8158055483238838", -1, -1, "8158055483238838", "false"},
                                    {18, 0, "522885093641679838", -1, -1, "522885093641679838", "false"},
                                    {18, 0, "608126450418190142", -1, -1, "608126450418190142", "false"},
                                    {18, 0, "712437226773593306", -1, -1, "712437226773593306", "false"},
                                    {18, 0, "560605504597905732", -1, -1, "560605504597905732", "false"},
                                    {18, 0, "-169889971512033548", -1, -1, "-169889971512033548", "false"},
                                    {18, 0, "950059291808033774", -1, -1, "950059291808033774", "false"},
                                    {18, 0, "-999999999999999998", -1, -1, "-999999999999999998", "false"},
                                    {18, 0, "-999710436339434850", -1, -1, "-999710436339434850", "false"},
                                    {18, 0, "-999999999886633581", -1, -1, "-999999999886633581", "false"},
                                    {18, 0, "2346984920271", -1, -1, "2346984920271", "false"},
                                    {18, 0, "165697470208983590", -1, -1, "165697470208983590", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToBooleanNormal) {
    CastTestCaseArray test_cases = {{18, 2, "-3516303764671658.95", -1, -1, "true", "false"},
                                    {18, 2, "-738757390176170.61", -1, -1, "true", "false"},
                                    {18, 2, "-6755660640809562.68", -1, -1, "true", "false"},
                                    {18, 2, "-5601187546800466.34", -1, -1, "true", "false"},
                                    {18, 2, "348068348755.99", -1, -1, "true", "false"},
                                    {18, 2, "-9999999999999999.52", -1, -1, "true", "false"},
                                    {18, 2, "2733618.57", -1, -1, "true", "false"},
                                    {18, 2, "397277585817544.25", -1, -1, "true", "false"},
                                    {18, 2, "-9983335990858563.31", -1, -1, "true", "false"},
                                    {18, 2, "1855654450174387.81", -1, -1, "true", "false"},
                                    {18, 2, "-5825724781381072.56", -1, -1, "true", "false"},
                                    {18, 2, "8127570330195043.00", -1, -1, "true", "false"},
                                    {18, 2, "1524471505187.89", -1, -1, "true", "false"},
                                    {18, 2, "-9999999999999995.93", -1, -1, "true", "false"},
                                    {18, 2, "-9999999999999997.47", -1, -1, "true", "false"},
                                    {18, 2, "-5517734741748207.04", -1, -1, "true", "false"},
                                    {18, 2, "-2886330925276510.47", -1, -1, "true", "false"},
                                    {18, 2, "7947404333076570.29", -1, -1, "true", "false"},
                                    {18, 2, "1289770635759746.88", -1, -1, "true", "false"},
                                    {18, 2, "-673213963818304.30", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{18, 2, "-6423139309778770.92", -1, -1, "", "true"},
                                    {18, 2, "937567.68", -1, -1, "", "true"},
                                    {18, 2, "814679373472069.74", -1, -1, "", "true"},
                                    {18, 2, "-9999916224847849.51", -1, -1, "", "true"},
                                    {18, 2, "11895.32", -1, -1, "", "true"},
                                    {18, 2, "-5489358654965620.64", -1, -1, "", "true"},
                                    {18, 2, "-2692892744720606.05", -1, -1, "", "true"},
                                    {18, 2, "2723.33", -1, -1, "", "true"},
                                    {18, 2, "-5184237354287538.32", -1, -1, "", "true"},
                                    {18, 2, "-8261490925156199.75", -1, -1, "", "true"},
                                    {18, 2, "-850888544896989.81", -1, -1, "", "true"},
                                    {18, 2, "77990.03", -1, -1, "", "true"},
                                    {18, 2, "-1715128236466623.70", -1, -1, "", "true"},
                                    {18, 2, "3173705624935623.69", -1, -1, "", "true"},
                                    {18, 2, "-1441979911109522.89", -1, -1, "", "true"},
                                    {18, 2, "-4122545466986446.09", -1, -1, "", "true"},
                                    {18, 2, "-10127270771514.64", -1, -1, "", "true"},
                                    {18, 2, "5970187986800646.95", -1, -1, "", "true"},
                                    {18, 2, "-9999999999673896.80", -1, -1, "", "true"},
                                    {18, 2, "-9010366251565069.50", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{18, 2, "-9088676629281066.36", -1, -1, "", "true"},
                                    {18, 2, "-5044267138183417.26", -1, -1, "", "true"},
                                    {18, 2, "7760403530841745.90", -1, -1, "", "true"},
                                    {18, 2, "2675650153078622.91", -1, -1, "", "true"},
                                    {18, 2, "44196748.17", -1, -1, "", "true"},
                                    {18, 2, "8470276664742826.15", -1, -1, "", "true"},
                                    {18, 2, "768006.85", -1, -1, "", "true"},
                                    {18, 2, "78120761.87", -1, -1, "", "true"},
                                    {18, 2, "3730677.80", -1, -1, "", "true"},
                                    {18, 2, "-3174664459898523.40", -1, -1, "", "true"},
                                    {18, 2, "-5191939598209377.36", -1, -1, "", "true"},
                                    {18, 2, "-7251149742856096.26", -1, -1, "", "true"},
                                    {18, 2, "-1187162122844165.51", -1, -1, "", "true"},
                                    {18, 2, "6096536977828496.97", -1, -1, "", "true"},
                                    {18, 2, "-9999876190588177.31", -1, -1, "", "true"},
                                    {18, 2, "2186735659418336.67", -1, -1, "", "true"},
                                    {18, 2, "-6160534431374286.31", -1, -1, "", "true"},
                                    {18, 2, "-9999998747595920.02", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{18, 2, "2455.08", -1, -1, "2455", "false"},
                                    {18, 2, "2454.42", -1, -1, "2454", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToIntAbnormal) {
    CastTestCaseArray test_cases = {
            {18, 2, "-2584541954542652.73", -1, -1, "", "true"}, {18, 2, "-4242116144986007.61", -1, -1, "", "true"},
            {18, 2, "-9999999966145189.05", -1, -1, "", "true"}, {18, 2, "-9999999999995762.60", -1, -1, "", "true"},
            {18, 2, "-4659823287248490.45", -1, -1, "", "true"}, {18, 2, "8904143093640277.62", -1, -1, "", "true"},
            {18, 2, "3091085696066192.32", -1, -1, "", "true"},  {18, 2, "-5185274768022819.41", -1, -1, "", "true"},
            {18, 2, "1877901663147158.19", -1, -1, "", "true"},  {18, 2, "7370470701526336.83", -1, -1, "", "true"},
            {18, 2, "7366605402361182.31", -1, -1, "", "true"},  {18, 2, "-9894643940549868.92", -1, -1, "", "true"},
            {18, 2, "1527024078834432.38", -1, -1, "", "true"},  {18, 2, "-2628668453931460.18", -1, -1, "", "true"},
            {18, 2, "-4211008212460367.32", -1, -1, "", "true"}, {18, 2, "-9997614163886960.47", -1, -1, "", "true"},
            {18, 2, "-6919165996903984.71", -1, -1, "", "true"}, {18, 2, "-9999999999999879.90", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToIntNormal) {
    CastTestCaseArray test_cases = {{18, 2, "35.39", -1, -1, "35", "false"}, {18, 2, "1.09", -1, -1, "1", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToBigIntNormal) {
    CastTestCaseArray test_cases = {{18, 2, "-8881374967791358.84", -1, -1, "-8881374967791358", "false"},
                                    {18, 2, "75180180.82", -1, -1, "75180180", "false"},
                                    {18, 2, "4854803942443021.73", -1, -1, "4854803942443021", "false"},
                                    {18, 2, "1430000121.07", -1, -1, "1430000121", "false"},
                                    {18, 2, "-3098619866277018.99", -1, -1, "-3098619866277018", "false"},
                                    {18, 2, "7048402450417441.20", -1, -1, "7048402450417441", "false"},
                                    {18, 2, "121410132859.82", -1, -1, "121410132859", "false"},
                                    {18, 2, "-9999999999915962.93", -1, -1, "-9999999999915962", "false"},
                                    {18, 2, "-2111193484118992.03", -1, -1, "-2111193484118992", "false"},
                                    {18, 2, "-7909912461849898.55", -1, -1, "-7909912461849898", "false"},
                                    {18, 2, "7921857456542465.46", -1, -1, "7921857456542465", "false"},
                                    {18, 2, "7233641889161354.69", -1, -1, "7233641889161354", "false"},
                                    {18, 2, "0.45", -1, -1, "0", "false"},
                                    {18, 2, "-9923322331923429.37", -1, -1, "-9923322331923429", "false"},
                                    {18, 2, "-9999999945131555.34", -1, -1, "-9999999945131555", "false"},
                                    {18, 2, "5592912122609751.72", -1, -1, "5592912122609751", "false"},
                                    {18, 2, "2193061.24", -1, -1, "2193061", "false"},
                                    {18, 2, "-7888550430657491.10", -1, -1, "-7888550430657491", "false"},
                                    {18, 2, "1214.84", -1, -1, "1214", "false"},
                                    {18, 2, "-9999999882238055.95", -1, -1, "-9999999882238055", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s2ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{18, 2, "9301064353124669.04", -1, -1, "9301064353124669", "false"},
                                    {18, 2, "-2559301710801450.91", -1, -1, "-2559301710801450", "false"},
                                    {18, 2, "9456.74", -1, -1, "9456", "false"},
                                    {18, 2, "-1666683557355009.57", -1, -1, "-1666683557355009", "false"},
                                    {18, 2, "62718279755.80", -1, -1, "62718279755", "false"},
                                    {18, 2, "883842465371126.08", -1, -1, "883842465371126", "false"},
                                    {18, 2, "8269055341451667.95", -1, -1, "8269055341451667", "false"},
                                    {18, 2, "-9999999999997175.38", -1, -1, "-9999999999997175", "false"},
                                    {18, 2, "8284871763533376.28", -1, -1, "8284871763533376", "false"},
                                    {18, 2, "24280111717754.20", -1, -1, "24280111717754", "false"},
                                    {18, 2, "67991880579407.57", -1, -1, "67991880579407", "false"},
                                    {18, 2, "636577.39", -1, -1, "636577", "false"},
                                    {18, 2, "-1715128236466623.70", -1, -1, "-1715128236466623", "false"},
                                    {18, 2, "-8426033905123514.83", -1, -1, "-8426033905123514", "false"},
                                    {18, 2, "-5756581833374659.25", -1, -1, "-5756581833374659", "false"},
                                    {18, 2, "-9999999979860092.36", -1, -1, "-9999999979860092", "false"},
                                    {18, 2, "-9999943685382042.93", -1, -1, "-9999943685382042", "false"},
                                    {18, 2, "618426701692.16", -1, -1, "618426701692", "false"},
                                    {18, 2, "-1787755468157464.84", -1, -1, "-1787755468157464", "false"},
                                    {18, 2, "5772983473636189.32", -1, -1, "5772983473636189", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToBooleanNormal) {
    CastTestCaseArray test_cases = {{18, 9, "-999999999.999999995", -1, -1, "true", "false"},
                                    {18, 9, "0.000002227", -1, -1, "true", "false"},
                                    {18, 9, "56271955.929929292", -1, -1, "true", "false"},
                                    {18, 9, "783728671.807350782", -1, -1, "true", "false"},
                                    {18, 9, "261657537.921911283", -1, -1, "true", "false"},
                                    {18, 9, "-564804312.726476575", -1, -1, "true", "false"},
                                    {18, 9, "1.642364631", -1, -1, "true", "false"},
                                    {18, 9, "-970223927.681578809", -1, -1, "true", "false"},
                                    {18, 9, "-241744546.139046125", -1, -1, "true", "false"},
                                    {18, 9, "-984531012.137547386", -1, -1, "true", "false"},
                                    {18, 9, "-609059011.497163927", -1, -1, "true", "false"},
                                    {18, 9, "0.051989995", -1, -1, "true", "false"},
                                    {18, 9, "228467935.264768460", -1, -1, "true", "false"},
                                    {18, 9, "420229750.290917920", -1, -1, "true", "false"},
                                    {18, 9, "721166342.018436695", -1, -1, "true", "false"},
                                    {18, 9, "3E-9", -1, -1, "true", "false"},
                                    {18, 9, "-999999997.689754296", -1, -1, "true", "false"},
                                    {18, 9, "-116424752.633864921", -1, -1, "true", "false"},
                                    {18, 9, "69180435.488089041", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {
            {18, 9, "-297345774.647918239", -1, -1, "", "true"}, {18, 9, "-778606735.579998306", -1, -1, "", "true"},
            {18, 9, "666732716.526857071", -1, -1, "", "true"},  {18, 9, "676727112.421276384", -1, -1, "", "true"},
            {18, 9, "-966530389.352052517", -1, -1, "", "true"}, {18, 9, "87388802.484309389", -1, -1, "", "true"},
            {18, 9, "-694772190.074461180", -1, -1, "", "true"}, {18, 9, "-999999772.587288637", -1, -1, "", "true"},
            {18, 9, "447552259.691125229", -1, -1, "", "true"},  {18, 9, "-999999999.990415238", -1, -1, "", "true"},
            {18, 9, "341884697.019871671", -1, -1, "", "true"},  {18, 9, "851580463.860158420", -1, -1, "", "true"},
            {18, 9, "-169375327.583618121", -1, -1, "", "true"}, {18, 9, "-920742835.368986619", -1, -1, "", "true"},
            {18, 9, "-733977245.473849833", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{18, 9, "0.000517208", -1, -1, "0", "false"},
                                    {18, 9, "15.644873794", -1, -1, "15", "false"},
                                    {18, 9, "1E-9", -1, -1, "0", "false"},
                                    {18, 9, "9.379208298", -1, -1, "9", "false"},
                                    {18, 9, "0E-9", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {
            {18, 9, "520961122.845045720", -1, -1, "", "true"},  {18, 9, "-358904626.285425555", -1, -1, "", "true"},
            {18, 9, "-999982692.176551743", -1, -1, "", "true"}, {18, 9, "-999228914.377508898", -1, -1, "", "true"},
            {18, 9, "-970185014.715217318", -1, -1, "", "true"}, {18, 9, "983934002.927329561", -1, -1, "", "true"},
            {18, 9, "476243132.091427848", -1, -1, "", "true"},  {18, 9, "-999999999.999875721", -1, -1, "", "true"},
            {18, 9, "-999999999.999999774", -1, -1, "", "true"}, {18, 9, "592446393.686482605", -1, -1, "", "true"},
            {18, 9, "-562829474.273795913", -1, -1, "", "true"}, {18, 9, "-171512823.646662370", -1, -1, "", "true"},
            {18, 9, "-999931086.730036695", -1, -1, "", "true"}, {18, 9, "385261107.222602193", -1, -1, "", "true"},
            {18, 9, "350365677.916912308", -1, -1, "", "true"},  {18, 9, "707910779.758392600", -1, -1, "", "true"},
            {18, 9, "507341561.153660965", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{18, 9, "0.000001663", -1, -1, "0", "false"},
                                    {18, 9, "0.005794181", -1, -1, "0", "false"},
                                    {18, 9, "13.024952385", -1, -1, "13", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToIntNormal) {
    CastTestCaseArray test_cases = {{18, 9, "202071339.049673641", -1, -1, "202071339", "false"},
                                    {18, 9, "-80254641.667233211", -1, -1, "-80254641", "false"},
                                    {18, 9, "800332118.024473251", -1, -1, "800332118", "false"},
                                    {18, 9, "0.000019593", -1, -1, "0", "false"},
                                    {18, 9, "-999999999.999966025", -1, -1, "-999999999", "false"},
                                    {18, 9, "-753693374.737044457", -1, -1, "-753693374", "false"},
                                    {18, 9, "292379634.835934234", -1, -1, "292379634", "false"},
                                    {18, 9, "451473142.727414440", -1, -1, "451473142", "false"},
                                    {18, 9, "-999999999.999999997", -1, -1, "-999999999", "false"},
                                    {18, 9, "0.006913527", -1, -1, "0", "false"},
                                    {18, 9, "-999999999.999999991", -1, -1, "-999999999", "false"},
                                    {18, 9, "624493151.532183142", -1, -1, "624493151", "false"},
                                    {18, 9, "1581042.497258432", -1, -1, "1581042", "false"},
                                    {18, 9, "5.701673068", -1, -1, "5", "false"},
                                    {18, 9, "0.211299626", -1, -1, "0", "false"},
                                    {18, 9, "664219820.065721260", -1, -1, "664219820", "false"},
                                    {18, 9, "-999999999.896389545", -1, -1, "-999999999", "false"},
                                    {18, 9, "484802642.152692878", -1, -1, "484802642", "false"},
                                    {18, 9, "775342298.847284313", -1, -1, "775342298", "false"},
                                    {18, 9, "-440638444.434172116", -1, -1, "-440638444", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToBigIntNormal) {
    CastTestCaseArray test_cases = {{18, 9, "-999999998.191997390", -1, -1, "-999999998", "false"},
                                    {18, 9, "6.246672473", -1, -1, "6", "false"},
                                    {18, 9, "426326113.160656470", -1, -1, "426326113", "false"},
                                    {18, 9, "319568604.712378807", -1, -1, "319568604", "false"},
                                    {18, 9, "-999999999.382791606", -1, -1, "-999999999", "false"},
                                    {18, 9, "-185679536.253286769", -1, -1, "-185679536", "false"},
                                    {18, 9, "0.000002064", -1, -1, "0", "false"},
                                    {18, 9, "486841280.906896500", -1, -1, "486841280", "false"},
                                    {18, 9, "-999999999.999986094", -1, -1, "-999999999", "false"},
                                    {18, 9, "-999999999.999944218", -1, -1, "-999999999", "false"},
                                    {18, 9, "-999999999.969549661", -1, -1, "-999999999", "false"},
                                    {18, 9, "-999999999.920679713", -1, -1, "-999999999", "false"},
                                    {18, 9, "-999944244.947347889", -1, -1, "-999944244", "false"},
                                    {18, 9, "-999946248.436917262", -1, -1, "-999946248", "false"},
                                    {18, 9, "-393695362.394953251", -1, -1, "-393695362", "false"},
                                    {18, 9, "2E-9", -1, -1, "0", "false"},
                                    {18, 9, "742591212.682975866", -1, -1, "742591212", "false"},
                                    {18, 9, "-369179608.232439417", -1, -1, "-369179608", "false"},
                                    {18, 9, "9050.423057871", -1, -1, "9050", "false"},
                                    {18, 9, "294439845.520562234", -1, -1, "294439845", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s9ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{18, 9, "41399258.902543369", -1, -1, "41399258", "false"},
                                    {18, 9, "-999999999.524508349", -1, -1, "-999999999", "false"},
                                    {18, 9, "944147425.111760666", -1, -1, "944147425", "false"},
                                    {18, 9, "-999999999.999649717", -1, -1, "-999999999", "false"},
                                    {18, 9, "-178724319.532062759", -1, -1, "-178724319", "false"},
                                    {18, 9, "107065700.186223000", -1, -1, "107065700", "false"},
                                    {18, 9, "-898284767.111782389", -1, -1, "-898284767", "false"},
                                    {18, 9, "0.027200807", -1, -1, "0", "false"},
                                    {18, 9, "0.135187042", -1, -1, "0", "false"},
                                    {18, 9, "-602124795.018899704", -1, -1, "-602124795", "false"},
                                    {18, 9, "-490642430.867420056", -1, -1, "-490642430", "false"},
                                    {18, 9, "-921565739.470298189", -1, -1, "-921565739", "false"},
                                    {18, 9, "-703730731.579301005", -1, -1, "-703730731", "false"},
                                    {18, 9, "-870700827.724668178", -1, -1, "-870700827", "false"},
                                    {18, 9, "-999996252.347778793", -1, -1, "-999996252", "false"},
                                    {18, 9, "-999999999.999765245", -1, -1, "-999999999", "false"},
                                    {18, 9, "-999999999.999280002", -1, -1, "-999999999", "false"},
                                    {18, 9, "-999999999.999996481", -1, -1, "-999999999", "false"},
                                    {18, 9, "834768995.428516939", -1, -1, "834768995", "false"},
                                    {18, 9, "33000149.084876372", -1, -1, "33000149", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s18ToBooleanNormal) {
    CastTestCaseArray test_cases = {{18, 18, "0.617079178485202252", -1, -1, "true", "false"},
                                    {18, 18, "-0.220836855328065832", -1, -1, "true", "false"},
                                    {18, 18, "0.058986311244883660", -1, -1, "true", "false"},
                                    {18, 18, "0.309222758304690302", -1, -1, "true", "false"},
                                    {18, 18, "-0.544425463157382515", -1, -1, "true", "false"},
                                    {18, 18, "0.684225324053570537", -1, -1, "true", "false"},
                                    {18, 18, "5E-18", -1, -1, "true", "false"},
                                    {18, 18, "-0.908598314285460281", -1, -1, "true", "false"},
                                    {18, 18, "2.9291149927E-8", -1, -1, "true", "false"},
                                    {18, 18, "-0.999999999996569891", -1, -1, "true", "false"},
                                    {18, 18, "4.75461E-13", -1, -1, "true", "false"},
                                    {18, 18, "2.6E-17", -1, -1, "true", "false"},
                                    {18, 18, "-0.895063604581107800", -1, -1, "true", "false"},
                                    {18, 18, "0.198979035360340889", -1, -1, "true", "false"},
                                    {18, 18, "-0.999916020034746119", -1, -1, "true", "false"},
                                    {18, 18, "0.272787423276193825", -1, -1, "true", "false"},
                                    {18, 18, "0.139385897153117188", -1, -1, "true", "false"},
                                    {18, 18, "0.429126463742926113", -1, -1, "true", "false"},
                                    {18, 18, "-0.480494061170228455", -1, -1, "true", "false"},
                                    {18, 18, "-0.999999999999999998", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s18ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{18, 18, "-0.999260279929234200", -1, -1, "0", "false"},
                                    {18, 18, "0.710014430171681390", -1, -1, "0", "false"},
                                    {18, 18, "1.873E-15", -1, -1, "0", "false"},
                                    {18, 18, "-0.306189187958514751", -1, -1, "0", "false"},
                                    {18, 18, "2.0421821160E-8", -1, -1, "0", "false"},
                                    {18, 18, "-0.999291152137715819", -1, -1, "0", "false"},
                                    {18, 18, "6.2645388E-11", -1, -1, "0", "false"},
                                    {18, 18, "-0.986415264970385819", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999999987", -1, -1, "0", "false"},
                                    {18, 18, "0.771561559274883396", -1, -1, "0", "false"},
                                    {18, 18, "1.56064674120E-7", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999974215920", -1, -1, "0", "false"},
                                    {18, 18, "5.17804E-13", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999999996", -1, -1, "0", "false"},
                                    {18, 18, "-0.034281555462335037", -1, -1, "0", "false"},
                                    {18, 18, "0.828487176353337628", -1, -1, "0", "false"},
                                    {18, 18, "0.000004262037706416", -1, -1, "0", "false"},
                                    {18, 18, "0.816804346316087138", -1, -1, "0", "false"},
                                    {18, 18, "9.617034484E-9", -1, -1, "0", "false"},
                                    {18, 18, "-0.425661875831192135", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s18ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{18, 18, "-0.998311062528103204", -1, -1, "0", "false"},
                                    {18, 18, "1.23338109893E-7", -1, -1, "0", "false"},
                                    {18, 18, "0.897224963944421710", -1, -1, "0", "false"},
                                    {18, 18, "0.758885801325367277", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999868872906", -1, -1, "0", "false"},
                                    {18, 18, "-0.300403259772022208", -1, -1, "0", "false"},
                                    {18, 18, "2.401E-15", -1, -1, "0", "false"},
                                    {18, 18, "0.798799588447289396", -1, -1, "0", "false"},
                                    {18, 18, "0.791458023794884664", -1, -1, "0", "false"},
                                    {18, 18, "0.929416711208299669", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999996994197", -1, -1, "0", "false"},
                                    {18, 18, "-0.934843683574630505", -1, -1, "0", "false"},
                                    {18, 18, "-0.505757344438332457", -1, -1, "0", "false"},
                                    {18, 18, "-0.646395833537231652", -1, -1, "0", "false"},
                                    {18, 18, "0.000001677677833583", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999999829", -1, -1, "0", "false"},
                                    {18, 18, "-0.024425421820562460", -1, -1, "0", "false"},
                                    {18, 18, "-0.714349584460865941", -1, -1, "0", "false"},
                                    {18, 18, "-0.551433771152364995", -1, -1, "0", "false"},
                                    {18, 18, "-0.999475422175509198", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s18ToIntNormal) {
    CastTestCaseArray test_cases = {{18, 18, "-0.624856085867120102", -1, -1, "0", "false"},
                                    {18, 18, "-0.999913964829159095", -1, -1, "0", "false"},
                                    {18, 18, "0.828487176353337628", -1, -1, "0", "false"},
                                    {18, 18, "0.602539373347857211", -1, -1, "0", "false"},
                                    {18, 18, "0.586935630510621316", -1, -1, "0", "false"},
                                    {18, 18, "-0.251723644042649245", -1, -1, "0", "false"},
                                    {18, 18, "-0.708731065905484455", -1, -1, "0", "false"},
                                    {18, 18, "-0.876461728968095141", -1, -1, "0", "false"},
                                    {18, 18, "-0.581525770012916933", -1, -1, "0", "false"},
                                    {18, 18, "-0.421586341519177183", -1, -1, "0", "false"},
                                    {18, 18, "0.698051704723880268", -1, -1, "0", "false"},
                                    {18, 18, "2.67E-16", -1, -1, "0", "false"},
                                    {18, 18, "-0.583502446598282070", -1, -1, "0", "false"},
                                    {18, 18, "-0.999944787730667820", -1, -1, "0", "false"},
                                    {18, 18, "0.389680374766523927", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999999409", -1, -1, "0", "false"},
                                    {18, 18, "4.11E-16", -1, -1, "0", "false"},
                                    {18, 18, "0.660197861557756878", -1, -1, "0", "false"},
                                    {18, 18, "-0.126003865604021225", -1, -1, "0", "false"},
                                    {18, 18, "-0.984585776400857940", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s18ToBigIntNormal) {
    CastTestCaseArray test_cases = {{18, 18, "0.442160295250977111", -1, -1, "0", "false"},
                                    {18, 18, "-0.141718650894921347", -1, -1, "0", "false"},
                                    {18, 18, "-0.648710554457976793", -1, -1, "0", "false"},
                                    {18, 18, "0.102298523201288639", -1, -1, "0", "false"},
                                    {18, 18, "1.2E-17", -1, -1, "0", "false"},
                                    {18, 18, "1.05857669949E-7", -1, -1, "0", "false"},
                                    {18, 18, "8.2626166730E-8", -1, -1, "0", "false"},
                                    {18, 18, "-0.999974184673795512", -1, -1, "0", "false"},
                                    {18, 18, "4.0017527298E-8", -1, -1, "0", "false"},
                                    {18, 18, "-0.304877571571785777", -1, -1, "0", "false"},
                                    {18, 18, "-0.360238554908759962", -1, -1, "0", "false"},
                                    {18, 18, "0.915309125216829234", -1, -1, "0", "false"},
                                    {18, 18, "0.812733715565780674", -1, -1, "0", "false"},
                                    {18, 18, "0.082849816184937078", -1, -1, "0", "false"},
                                    {18, 18, "-0.984334465327939155", -1, -1, "0", "false"},
                                    {18, 18, "7.29685E-13", -1, -1, "0", "false"},
                                    {18, 18, "0.918042341256853351", -1, -1, "0", "false"},
                                    {18, 18, "-0.814620480821615688", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999997842165", -1, -1, "0", "false"},
                                    {18, 18, "2.51963874E-10", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p18s18ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{18, 18, "-0.999999999660282886", -1, -1, "0", "false"},
                                    {18, 18, "-0.728273938542645262", -1, -1, "0", "false"},
                                    {18, 18, "-0.779812752373120967", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999999984", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999924429172", -1, -1, "0", "false"},
                                    {18, 18, "0.956529258999259890", -1, -1, "0", "false"},
                                    {18, 18, "-0.456222395867814003", -1, -1, "0", "false"},
                                    {18, 18, "-0.978529132324824413", -1, -1, "0", "false"},
                                    {18, 18, "-0.819898306460819590", -1, -1, "0", "false"},
                                    {18, 18, "0.717484115218506274", -1, -1, "0", "false"},
                                    {18, 18, "2.5584E-14", -1, -1, "0", "false"},
                                    {18, 18, "0.439846481037758814", -1, -1, "0", "false"},
                                    {18, 18, "0.343040561126369424", -1, -1, "0", "false"},
                                    {18, 18, "-0.979982771382813279", -1, -1, "0", "false"},
                                    {18, 18, "7.06186E-13", -1, -1, "0", "false"},
                                    {18, 18, "-0.164226204867532402", -1, -1, "0", "false"},
                                    {18, 18, "2.250E-15", -1, -1, "0", "false"},
                                    {18, 18, "1.667674E-12", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999874782", -1, -1, "0", "false"},
                                    {18, 18, "-0.999999999999909772", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p15s13ToBooleanNormal) {
    CastTestCaseArray test_cases = {{15, 13, "61.5260802472523", -1, -1, "true", "false"},
                                    {15, 13, "33.0503315346044", -1, -1, "true", "false"},
                                    {15, 13, "-99.9999889752650", -1, -1, "true", "false"},
                                    {15, 13, "-99.9999497256347", -1, -1, "true", "false"},
                                    {15, 13, "-66.9718389563401", -1, -1, "true", "false"},
                                    {15, 13, "4.42E-11", -1, -1, "true", "false"},
                                    {15, 13, "-6.2386839564974", -1, -1, "true", "false"},
                                    {15, 13, "-9.2584597171927", -1, -1, "true", "false"},
                                    {15, 13, "0.0000014936331", -1, -1, "true", "false"},
                                    {15, 13, "-39.8187276894784", -1, -1, "true", "false"},
                                    {15, 13, "-99.9999999999975", -1, -1, "true", "false"},
                                    {15, 13, "31.0528774263433", -1, -1, "true", "false"},
                                    {15, 13, "-99.3146928098460", -1, -1, "true", "false"},
                                    {15, 13, "8.4261287801554", -1, -1, "true", "false"},
                                    {15, 13, "-24.6338841010606", -1, -1, "true", "false"},
                                    {15, 13, "-20.8245704031565", -1, -1, "true", "false"},
                                    {15, 13, "91.9491687528696", -1, -1, "true", "false"},
                                    {15, 13, "-84.5483206230623", -1, -1, "true", "false"},
                                    {15, 13, "36.7835899366713", -1, -1, "true", "false"},
                                    {15, 13, "67.8532750365797", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p15s13ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{15, 13, "-70.6254700345194", -1, -1, "-70", "false"},
                                    {15, 13, "-99.9999999972094", -1, -1, "-99", "false"},
                                    {15, 13, "-99.9999983696129", -1, -1, "-99", "false"},
                                    {15, 13, "-82.4134184351198", -1, -1, "-82", "false"},
                                    {15, 13, "9.3876408585318", -1, -1, "9", "false"},
                                    {15, 13, "-30.2738408354041", -1, -1, "-30", "false"},
                                    {15, 13, "-99.3742087333492", -1, -1, "-99", "false"},
                                    {15, 13, "-6.4927024156177", -1, -1, "-6", "false"},
                                    {15, 13, "6.6709552158253", -1, -1, "6", "false"},
                                    {15, 13, "1.4157147607029", -1, -1, "1", "false"},
                                    {15, 13, "-75.9803108854498", -1, -1, "-75", "false"},
                                    {15, 13, "23.9831150640215", -1, -1, "23", "false"},
                                    {15, 13, "68.3342470881005", -1, -1, "68", "false"},
                                    {15, 13, "-82.9967705541197", -1, -1, "-82", "false"},
                                    {15, 13, "-53.7561026331500", -1, -1, "-53", "false"},
                                    {15, 13, "2.0659E-9", -1, -1, "0", "false"},
                                    {15, 13, "-62.8833769381309", -1, -1, "-62", "false"},
                                    {15, 13, "2.7504250940616", -1, -1, "2", "false"},
                                    {15, 13, "-99.9990655253798", -1, -1, "-99", "false"},
                                    {15, 13, "96.2501727063072", -1, -1, "96", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p15s13ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{15, 13, "-99.9996294941128", -1, -1, "-99", "false"},
                                    {15, 13, "1.7930436926145", -1, -1, "1", "false"},
                                    {15, 13, "27.9524225912648", -1, -1, "27", "false"},
                                    {15, 13, "-99.9999971650544", -1, -1, "-99", "false"},
                                    {15, 13, "-99.9999999999997", -1, -1, "-99", "false"},
                                    {15, 13, "65.8453358813240", -1, -1, "65", "false"},
                                    {15, 13, "-65.6604386056943", -1, -1, "-65", "false"},
                                    {15, 13, "4.002048E-7", -1, -1, "0", "false"},
                                    {15, 13, "2.6754939323590", -1, -1, "2", "false"},
                                    {15, 13, "-99.9999997703758", -1, -1, "-99", "false"},
                                    {15, 13, "0.0000496812396", -1, -1, "0", "false"},
                                    {15, 13, "79.8703734837319", -1, -1, "79", "false"},
                                    {15, 13, "-80.2499356193467", -1, -1, "-80", "false"},
                                    {15, 13, "22.2536462059457", -1, -1, "22", "false"},
                                    {15, 13, "88.9564734471324", -1, -1, "88", "false"},
                                    {15, 13, "-32.8953517103645", -1, -1, "-32", "false"},
                                    {15, 13, "-44.6000428273453", -1, -1, "-44", "false"},
                                    {15, 13, "64.9434366203421", -1, -1, "64", "false"},
                                    {15, 13, "58.9262933682572", -1, -1, "58", "false"},
                                    {15, 13, "-99.9039795829659", -1, -1, "-99", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p15s13ToIntNormal) {
    CastTestCaseArray test_cases = {{15, 13, "-76.6676562214500", -1, -1, "-76", "false"},
                                    {15, 13, "-99.9999999898452", -1, -1, "-99", "false"},
                                    {15, 13, "-99.9999999999957", -1, -1, "-99", "false"},
                                    {15, 13, "-0.8845205340775", -1, -1, "0", "false"},
                                    {15, 13, "-99.9999999633825", -1, -1, "-99", "false"},
                                    {15, 13, "-37.2883988785006", -1, -1, "-37", "false"},
                                    {15, 13, "-41.6118059059593", -1, -1, "-41", "false"},
                                    {15, 13, "-99.9999999807753", -1, -1, "-99", "false"},
                                    {15, 13, "-30.4338196477519", -1, -1, "-30", "false"},
                                    {15, 13, "37.9291493797792", -1, -1, "37", "false"},
                                    {15, 13, "-66.1739178655766", -1, -1, "-66", "false"},
                                    {15, 13, "-11.3025029520407", -1, -1, "-11", "false"},
                                    {15, 13, "50.6558957057938", -1, -1, "50", "false"},
                                    {15, 13, "58.2975066252371", -1, -1, "58", "false"},
                                    {15, 13, "5.516E-10", -1, -1, "0", "false"},
                                    {15, 13, "-21.7027539205785", -1, -1, "-21", "false"},
                                    {15, 13, "0.8411187501404", -1, -1, "0", "false"},
                                    {15, 13, "41.1389664423546", -1, -1, "41", "false"},
                                    {15, 13, "-99.9999999400077", -1, -1, "-99", "false"},
                                    {15, 13, "9.3210311059892", -1, -1, "9", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p15s13ToBigIntNormal) {
    CastTestCaseArray test_cases = {{15, 13, "-99.9999990571464", -1, -1, "-99", "false"},
                                    {15, 13, "60.7928935098342", -1, -1, "60", "false"},
                                    {15, 13, "3.1538503869151", -1, -1, "3", "false"},
                                    {15, 13, "2E-13", -1, -1, "0", "false"},
                                    {15, 13, "12.5330278726217", -1, -1, "12", "false"},
                                    {15, 13, "-97.2496131588758", -1, -1, "-97", "false"},
                                    {15, 13, "-69.3001277223247", -1, -1, "-69", "false"},
                                    {15, 13, "-79.7691450196038", -1, -1, "-79", "false"},
                                    {15, 13, "32.3414965828531", -1, -1, "32", "false"},
                                    {15, 13, "1.6E-12", -1, -1, "0", "false"},
                                    {15, 13, "-99.9999962034998", -1, -1, "-99", "false"},
                                    {15, 13, "-98.2861179563745", -1, -1, "-98", "false"},
                                    {15, 13, "5.1349940474465", -1, -1, "5", "false"},
                                    {15, 13, "-23.6871815355108", -1, -1, "-23", "false"},
                                    {15, 13, "-45.0675649933961", -1, -1, "-45", "false"},
                                    {15, 13, "-99.9999999999981", -1, -1, "-99", "false"},
                                    {15, 13, "1.4205E-9", -1, -1, "0", "false"},
                                    {15, 13, "67.8742171727607", -1, -1, "67", "false"},
                                    {15, 13, "-99.9601423994753", -1, -1, "-99", "false"},
                                    {15, 13, "0.0174911142486", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal64p15s13ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{15, 13, "96.9708261594545", -1, -1, "96", "false"},
                                    {15, 13, "4.9313025661834", -1, -1, "4", "false"},
                                    {15, 13, "4.79E-11", -1, -1, "0", "false"},
                                    {15, 13, "-22.3429172437559", -1, -1, "-22", "false"},
                                    {15, 13, "-69.0104930868824", -1, -1, "-69", "false"},
                                    {15, 13, "-50.2025004939778", -1, -1, "-50", "false"},
                                    {15, 13, "-44.9508456541565", -1, -1, "-44", "false"},
                                    {15, 13, "25.0656346718350", -1, -1, "25", "false"},
                                    {15, 13, "-99.9999999997017", -1, -1, "-99", "false"},
                                    {15, 13, "-65.8107036161260", -1, -1, "-65", "false"},
                                    {15, 13, "81.1313784859449", -1, -1, "81", "false"},
                                    {15, 13, "75.9772888187053", -1, -1, "75", "false"},
                                    {15, 13, "79.5946733744341", -1, -1, "79", "false"},
                                    {15, 13, "0.1034539460498", -1, -1, "0", "false"},
                                    {15, 13, "-23.5814714021401", -1, -1, "-23", "false"},
                                    {15, 13, "-69.2969551297625", -1, -1, "-69", "false"},
                                    {15, 13, "79.9220756422000", -1, -1, "79", "false"},
                                    {15, 13, "-71.5588345309580", -1, -1, "-71", "false"},
                                    {15, 13, "37.6169815341898", -1, -1, "37", "false"},
                                    {15, 13, "-23.6713761199951", -1, -1, "-23", "false"}};
    test_cast_all<TYPE_DECIMAL64, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToBooleanNormal) {
    CastTestCaseArray test_cases = {{38, 0, "4820287502054252819807570", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999999999405571410478", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999999999615430720867", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999999999987183621516", -1, -1, "true", "false"},
                                    {38, 0, "132411988799732011818550387", -1, -1, "true", "false"},
                                    {38, 0, "-99991877149657803870440332839639552385", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999969021421886333084415", -1, -1, "true", "false"},
                                    {38, 0, "717992584680249337162645717", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999997976586660968821", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999920427393992129905150", -1, -1, "true", "false"},
                                    {38, 0, "55556169", -1, -1, "true", "false"},
                                    {38, 0, "-81075540590908228267046755078880464091", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999999998682840072813", -1, -1, "true", "false"},
                                    {38, 0, "-99106057026376519898960489128752645948", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999999999999979870064", -1, -1, "true", "false"},
                                    {38, 0, "3218788585066463989843538", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999976909876754783359083298594", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999999681704144517445", -1, -1, "true", "false"},
                                    {38, 0, "-99999999999999999999426221350357365749", -1, -1, "true", "false"},
                                    {38, 0, "-99999999627339996502126897092235229745", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 0, "840651753914743083904802", -1, -1, "", "true"},
                                    {38, 0, "493384589550209011985172293957412535", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999999999999835", -1, -1, "", "true"},
                                    {38, 0, "-99944269896817461460553516949647949053", -1, -1, "", "true"},
                                    {38, 0, "9474218350870555801801598", -1, -1, "", "true"},
                                    {38, 0, "126094701777520987802", -1, -1, "", "true"},
                                    {38, 0, "1633248869990139910115728050640540570", -1, -1, "", "true"},
                                    {38, 0, "-99991376064699876408360307111308407754", -1, -1, "", "true"},
                                    {38, 0, "4030227221910885904207910570", -1, -1, "", "true"},
                                    {38, 0, "-99999999998947198982488926215114180766", -1, -1, "", "true"},
                                    {38, 0, "4017913718", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999992705272928", -1, -1, "", "true"},
                                    {38, 0, "-99999999999428499871471725019641843139", -1, -1, "", "true"},
                                    {38, 0, "-99999999995132036789947139098018562320", -1, -1, "", "true"},
                                    {38, 0, "-99999997649000476890608898911614190901", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999755276573722806650702533", -1, -1, "", "true"},
                                    {38, 0, "9329441238758279239896209229", -1, -1, "", "true"},
                                    {38, 0, "2919614670613289598525704966872", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999896947152081749", -1, -1, "", "true"},
                                    {38, 0, "546716", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 0, "423617522101170279955475405749", -1, -1, "", "true"},
                                    {38, 0, "726116374303001912", -1, -1, "", "true"},
                                    {38, 0, "-99999999999993756058206155545162733720", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999983549991084181020823", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999999999999994", -1, -1, "", "true"},
                                    {38, 0, "-99983165276635402957697346951215457099", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999597794308987652498116", -1, -1, "", "true"},
                                    {38, 0, "318444138374472203319871769443762496", -1, -1, "", "true"},
                                    {38, 0, "59040679678738656013792266261254", -1, -1, "", "true"},
                                    {38, 0, "1380324", -1, -1, "", "true"},
                                    {38, 0, "-58726684708866490011355849203079806469", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999984510603469601250553", -1, -1, "", "true"},
                                    {38, 0, "-99997845459585493762370903996483953686", -1, -1, "", "true"},
                                    {38, 0, "412823", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999967752670365737276", -1, -1, "", "true"},
                                    {38, 0, "8640430247950635261139", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999948599560992234189054015", -1, -1, "", "true"},
                                    {38, 0, "-99993990883308118678900603798113126574", -1, -1, "", "true"},
                                    {38, 0, "-99999998948592133978429393400897104227", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{38, 0, "482", -1, -1, "482", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 0, "47508562399284233", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999998476237730", -1, -1, "", "true"},
                                    {38, 0, "32719660562964216", -1, -1, "", "true"},
                                    {38, 0, "-88518332648594317469914535963819004578", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999996342888287337818", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999493968332646507983435", -1, -1, "", "true"},
                                    {38, 0, "33723913020718804097836544300505025", -1, -1, "", "true"},
                                    {38, 0, "4354400428325395207972221888676849", -1, -1, "", "true"},
                                    {38, 0, "123085895441934602986536", -1, -1, "", "true"},
                                    {38, 0, "-99933737109709003064502332330777589273", -1, -1, "", "true"},
                                    {38, 0, "4765191118770414111548266138251997342", -1, -1, "", "true"},
                                    {38, 0, "6965749698232589938088596409", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999993096958229348", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999819120477634601", -1, -1, "", "true"},
                                    {38, 0, "564005242560690651656711710177314", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToIntNormal) {
    CastTestCaseArray test_cases = {{38, 0, "3011182", -1, -1, "3011182", "false"},
                                    {38, 0, "4176133", -1, -1, "4176133", "false"},
                                    {38, 0, "365", -1, -1, "365", "false"},
                                    {38, 0, "12553838", -1, -1, "12553838", "false"},
                                    {38, 0, "656215", -1, -1, "656215", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToBigIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 0, "-99999999999999999774146723441589216466", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999980538895797312123", -1, -1, "", "true"},
                                    {38, 0, "-99993736478374380030709958867806497042", -1, -1, "", "true"},
                                    {38, 0, "404862041063479666938701596", -1, -1, "", "true"},
                                    {38, 0, "608892144526786579238231118196", -1, -1, "", "true"},
                                    {38, 0, "-99645490572207290289165439011747530964", -1, -1, "", "true"},
                                    {38, 0, "-99999999999867636751472314794841629847", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999999999938226", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999998958869983555276879491", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999517216109374302313", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999247783568889", -1, -1, "", "true"},
                                    {38, 0, "-99999999999999999999999999999984348286", -1, -1, "", "true"},
                                    {38, 0, "-99999999884134700962466574862090658109", -1, -1, "", "true"},
                                    {38, 0, "-99839765241552395086775862006573488734", -1, -1, "", "true"},
                                    {38, 0, "565004871511049431356739720748", -1, -1, "", "true"},
                                    {38, 0, "-64191220588719036062454729943889367596", -1, -1, "", "true"},
                                    {38, 0, "2209191617299608558625759", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToBigIntNormal) {
    CastTestCaseArray test_cases = {{38, 0, "35746232042337387", -1, -1, "35746232042337387", "false"},
                                    {38, 0, "12019751000", -1, -1, "12019751000", "false"},
                                    {38, 0, "1", -1, -1, "1", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s0ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 0, "1246608920367585820387537429344317406", -1, -1, "1246608920367585820387537429344317406", "false"},
            {38, 0, "374542", -1, -1, "374542", "false"},
            {38, 0, "42351078736110414579168", -1, -1, "42351078736110414579168", "false"},
            {38, 0, "-99999999999999999999999999999999992474", -1, -1, "-99999999999999999999999999999999992474",
             "false"},
            {38, 0, "-99999999999999999999999985668833836065", -1, -1, "-99999999999999999999999985668833836065",
             "false"},
            {38, 0, "-99999999999999999999999999277868483648", -1, -1, "-99999999999999999999999999277868483648",
             "false"},
            {38, 0, "-99999999999999999999999999999999999987", -1, -1, "-99999999999999999999999999999999999987",
             "false"},
            {38, 0, "5012938810375474", -1, -1, "5012938810375474", "false"},
            {38, 0, "-99999999999981466869495186332371125520", -1, -1, "-99999999999981466869495186332371125520",
             "false"},
            {38, 0, "320163579", -1, -1, "320163579", "false"},
            {38, 0, "62491856", -1, -1, "62491856", "false"},
            {38, 0, "2264126806105846893509014945990888", -1, -1, "2264126806105846893509014945990888", "false"},
            {38, 0, "1298", -1, -1, "1298", "false"},
            {38, 0, "12292412230239570011338493641892870450", -1, -1, "12292412230239570011338493641892870450",
             "false"},
            {38, 0, "-99999999999999999999977511041264155781", -1, -1, "-99999999999999999999977511041264155781",
             "false"},
            {38, 0, "85102891323158241", -1, -1, "85102891323158241", "false"},
            {38, 0, "-99999999999999999999999999999999999975", -1, -1, "-99999999999999999999999999999999999975",
             "false"},
            {38, 0, "1972633595186724190146901866308471", -1, -1, "1972633595186724190146901866308471", "false"},
            {38, 0, "54852144746887833163593526", -1, -1, "54852144746887833163593526", "false"},
            {38, 0, "-99999338841041945349573135511758895461", -1, -1, "-99999338841041945349573135511758895461",
             "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToBooleanNormal) {
    CastTestCaseArray test_cases = {{38, 2, "-999999999999999999999999952235069117.55", -1, -1, "true", "false"},
                                    {38, 2, "1264728562099.27", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999998271084397519704849477.35", -1, -1, "true", "false"},
                                    {38, 2, "1.49", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999999999999998772353533950.38", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999999999529679066931892759.75", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999999999999999999801874578.34", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999999999999999999999999989.91", -1, -1, "true", "false"},
                                    {38, 2, "2799588725654555732985976192121.66", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999999999999999999995984441.56", -1, -1, "true", "false"},
                                    {38, 2, "-999999999972285294232141983262863132.20", -1, -1, "true", "false"},
                                    {38, 2, "100650143674200983515155154740222794.63", -1, -1, "true", "false"},
                                    {38, 2, "281646.62", -1, -1, "true", "false"},
                                    {38, 2, "599355561623562035391.61", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999999999999994940510973927.96", -1, -1, "true", "false"},
                                    {38, 2, "2083896971544925146.55", -1, -1, "true", "false"},
                                    {38, 2, "267.18", -1, -1, "true", "false"},
                                    {38, 2, "-999999999999990631718921702416845656.97", -1, -1, "true", "false"},
                                    {38, 2, "-999998499815891845019863042187082841.06", -1, -1, "true", "false"},
                                    {38, 2, "124.24", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 2, "-999999999999999994313787362529975290.50", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999937885592042738.88", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999938886210318.75", -1, -1, "", "true"},
                                    {38, 2, "282949499541982950.61", -1, -1, "", "true"},
                                    {38, 2, "-999763395929627387954952571800375958.34", -1, -1, "", "true"},
                                    {38, 2, "-999999999999964579798097102426548417.69", -1, -1, "", "true"},
                                    {38, 2, "-999999089614260826834280293789253316.38", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999947337737.93", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999999998.73", -1, -1, "", "true"},
                                    {38, 2, "-989144952232749235082475789354882650.95", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999999882.33", -1, -1, "", "true"},
                                    {38, 2, "-999999999999978634955505622350502742.03", -1, -1, "", "true"},
                                    {38, 2, "16054895536706584287.28", -1, -1, "", "true"},
                                    {38, 2, "165252668.40", -1, -1, "", "true"},
                                    {38, 2, "371712868502422.17", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999999994.03", -1, -1, "", "true"},
                                    {38, 2, "620466979808.62", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999999944.51", -1, -1, "", "true"},
                                    {38, 2, "122226185374585.30", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{38, 2, "0.03", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 2, "-999999999999999999999999705877889783.01", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999843830431.53", -1, -1, "", "true"},
                                    {38, 2, "9264133115479260898386023.01", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999999850.09", -1, -1, "", "true"},
                                    {38, 2, "10992677895552723.13", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999997528797.91", -1, -1, "", "true"},
                                    {38, 2, "92753532567709567508087010216.31", -1, -1, "", "true"},
                                    {38, 2, "41701.70", -1, -1, "", "true"},
                                    {38, 2, "16609718120457.73", -1, -1, "", "true"},
                                    {38, 2, "14985401978782.88", -1, -1, "", "true"},
                                    {38, 2, "7991540676674871707760468.57", -1, -1, "", "true"},
                                    {38, 2, "-999999999630433442983730047970016988.13", -1, -1, "", "true"},
                                    {38, 2, "91418631.18", -1, -1, "", "true"},
                                    {38, 2, "91662051984.47", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999714156619965716784.08", -1, -1, "", "true"},
                                    {38, 2, "-998571257948159157207468384614024116.55", -1, -1, "", "true"},
                                    {38, 2, "25293712491537665811307722255741.92", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{38, 2, "0.00", -1, -1, "0", "false"},
                                    {38, 2, "6.82", -1, -1, "6", "false"},
                                    {38, 2, "38.96", -1, -1, "38", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 2, "38225259973028822769847757164935434.46", -1, -1, "", "true"},
                                    {38, 2, "-999993299298244145017019377337305695.50", -1, -1, "", "true"},
                                    {38, 2, "1172893106737712172909414970840720.50", -1, -1, "", "true"},
                                    {38, 2, "-999999935269195641552640991196889463.54", -1, -1, "", "true"},
                                    {38, 2, "-960604229738640452062313894575482683.79", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999684558845474763803390.75", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999997103838234663.69", -1, -1, "", "true"},
                                    {38, 2, "3967554199759805228347985.19", -1, -1, "", "true"},
                                    {38, 2, "34530356400026259235470118.57", -1, -1, "", "true"},
                                    {38, 2, "-999999999999811470247148737663378218.45", -1, -1, "", "true"},
                                    {38, 2, "424351312957725751833171296.83", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999315974105134.13", -1, -1, "", "true"},
                                    {38, 2, "4709977623349286.20", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999855783.64", -1, -1, "", "true"},
                                    {38, 2, "113780208827426032921177.20", -1, -1, "", "true"},
                                    {38, 2, "6070860791849259925558982850925878.37", -1, -1, "", "true"},
                                    {38, 2, "-999264584424789432184093232722881776.05", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToIntNormal) {
    CastTestCaseArray test_cases = {{38, 2, "262.03", -1, -1, "262", "false"},
                                    {38, 2, "63568.22", -1, -1, "63568", "false"},
                                    {38, 2, "28262574.32", -1, -1, "28262574", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToBigIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 2, "-999999999999999999999999999999999996.12", -1, -1, "", "true"},
                                    {38, 2, "-999999999997117766008521999342094288.19", -1, -1, "", "true"},
                                    {38, 2, "-999999543697550630336218356266203055.75", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999085916914061948909514.87", -1, -1, "", "true"},
                                    {38, 2, "901390659074460659914.45", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999709712008905.27", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999520577.86", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999965545204254574.11", -1, -1, "", "true"},
                                    {38, 2, "-999999360020202605867034094209894474.02", -1, -1, "", "true"},
                                    {38, 2, "-999997134628740162002910766800118935.57", -1, -1, "", "true"},
                                    {38, 2, "-999331482854830988782014342680752562.05", -1, -1, "", "true"},
                                    {38, 2, "-999999999999999999999999999999999999.97", -1, -1, "", "true"},
                                    {38, 2, "3200261517576174426392557.60", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToBigIntNormal) {
    CastTestCaseArray test_cases = {{38, 2, "20642033673.86", -1, -1, "20642033673", "false"},
                                    {38, 2, "134801533.70", -1, -1, "134801533", "false"},
                                    {38, 2, "7089147991485724.55", -1, -1, "7089147991485724", "false"},
                                    {38, 2, "160.79", -1, -1, "160", "false"},
                                    {38, 2, "16.57", -1, -1, "16", "false"},
                                    {38, 2, "4286.39", -1, -1, "4286", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s2ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 2, "4318016959330735.42", -1, -1, "4318016959330735", "false"},
            {38, 2, "0.01", -1, -1, "0", "false"},
            {38, 2, "-999999999999999999999633543696978937.23", -1, -1, "-999999999999999999999633543696978937",
             "false"},
            {38, 2, "4124135199930274.42", -1, -1, "4124135199930274", "false"},
            {38, 2, "25997901471937753460702381730619.80", -1, -1, "25997901471937753460702381730619", "false"},
            {38, 2, "-999999999999999999999999999999999999.93", -1, -1, "-999999999999999999999999999999999999",
             "false"},
            {38, 2, "-999999999999995214249725909001871801.22", -1, -1, "-999999999999995214249725909001871801",
             "false"},
            {38, 2, "-298588165395307682683126962841158942.70", -1, -1, "-298588165395307682683126962841158942",
             "false"},
            {38, 2, "12764099065420422495783249405575.54", -1, -1, "12764099065420422495783249405575", "false"},
            {38, 2, "2309989.86", -1, -1, "2309989", "false"},
            {38, 2, "1336250021218.45", -1, -1, "1336250021218", "false"},
            {38, 2, "-999957655863015650331823179297518729.49", -1, -1, "-999957655863015650331823179297518729",
             "false"},
            {38, 2, "36886653547176675845.30", -1, -1, "36886653547176675845", "false"},
            {38, 2, "-999999999999981903454995383201635766.18", -1, -1, "-999999999999981903454995383201635766",
             "false"},
            {38, 2, "-999999999998875344350289196759090140.58", -1, -1, "-999999999998875344350289196759090140",
             "false"},
            {38, 2, "689103174812715.90", -1, -1, "689103174812715", "false"},
            {38, 2, "-999999407429766291490915868013793433.15", -1, -1, "-999999407429766291490915868013793433",
             "false"},
            {38, 2, "-999999999999999999491892875756946524.91", -1, -1, "-999999999999999999491892875756946524",
             "false"},
            {38, 2, "14930775961550739.46", -1, -1, "14930775961550739", "false"},
            {38, 2, "276332815.50", -1, -1, "276332815", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToBooleanNormal) {
    CastTestCaseArray test_cases = {{38, 9, "-99999999999999999999999573619.892528566", -1, -1, "true", "false"},
                                    {38, 9, "707486638209800.322140396", -1, -1, "true", "false"},
                                    {38, 9, "0.000010977", -1, -1, "true", "false"},
                                    {38, 9, "0.000003101", -1, -1, "true", "false"},
                                    {38, 9, "-99999999999999999999999346856.667866976", -1, -1, "true", "false"},
                                    {38, 9, "-99999999999999999999999999884.851108112", -1, -1, "true", "false"},
                                    {38, 9, "-99903919933433561641897882671.430789663", -1, -1, "true", "false"},
                                    {38, 9, "-99999999999999999999755113659.425136794", -1, -1, "true", "false"},
                                    {38, 9, "1652411555792437541560816093.058521164", -1, -1, "true", "false"},
                                    {38, 9, "-99999999999999592705215911131.956908779", -1, -1, "true", "false"},
                                    {38, 9, "-99999999999991344274824325120.260220320", -1, -1, "true", "false"},
                                    {38, 9, "778853.921970692", -1, -1, "true", "false"},
                                    {38, 9, "129707412292128610536310569.519135204", -1, -1, "true", "false"},
                                    {38, 9, "13327.606989863", -1, -1, "true", "false"},
                                    {38, 9, "-99999999999999999999999999999.999999783", -1, -1, "true", "false"},
                                    {38, 9, "4.20E-7", -1, -1, "true", "false"},
                                    {38, 9, "29584886035833536.235576701", -1, -1, "true", "false"},
                                    {38, 9, "1184107095028552.518372480", -1, -1, "true", "false"},
                                    {38, 9, "0.044974665", -1, -1, "true", "false"},
                                    {38, 9, "882733012.686079963", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 9, "-99999999999999999999999999999.999999992", -1, -1, "", "true"},
                                    {38, 9, "-99999999949050478693550601540.725207161", -1, -1, "", "true"},
                                    {38, 9, "-99994497672491644880728325285.068583767", -1, -1, "", "true"},
                                    {38, 9, "4135344644348.519311334", -1, -1, "", "true"},
                                    {38, 9, "261998261993092146523.150726478", -1, -1, "", "true"},
                                    {38, 9, "69299258.431290883", -1, -1, "", "true"},
                                    {38, 9, "-98244583327321145386073380622.640009384", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999997774.756911423", -1, -1, "", "true"},
                                    {38, 9, "14882930686191253.398417741", -1, -1, "", "true"},
                                    {38, 9, "22990297367756897774751.682598093", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999997299213.714430771", -1, -1, "", "true"},
                                    {38, 9, "70141183460469231731687303715.884105728", -1, -1, "", "true"},
                                    {38, 9, "1672233675432729595722.781326227", -1, -1, "", "true"},
                                    {38, 9, "198814232847760385.291332211", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999391159152863.323952245", -1, -1, "", "true"},
                                    {38, 9, "-99999999999871015734040102888.431652652", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999968990312.599400431", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999948410", -1, -1, "", "true"},
                                    {38, 9, "324898055513.245129622", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{38, 9, "0.036724045", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 9, "7745490475221289.784947866", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999622.656310471", -1, -1, "", "true"},
                                    {38, 9, "41045738588795914748.068319196", -1, -1, "", "true"},
                                    {38, 9, "-99999999934403983003740500379.101067498", -1, -1, "", "true"},
                                    {38, 9, "25253080269977291928606050.669948633", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999995.871915082", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999796500", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999999249", -1, -1, "", "true"},
                                    {38, 9, "378256.433348454", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999999372", -1, -1, "", "true"},
                                    {38, 9, "-99990635949924609649046137859.704416108", -1, -1, "", "true"},
                                    {38, 9, "-92986795742549781482082095993.624292586", -1, -1, "", "true"},
                                    {38, 9, "-99999999999997859783773996312.196828022", -1, -1, "", "true"},
                                    {38, 9, "39150.691300999", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999987302", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{38, 9, "0.000044861", -1, -1, "0", "false"},
                                    {38, 9, "0.000116653", -1, -1, "0", "false"},
                                    {38, 9, "251.506461337", -1, -1, "251", "false"},
                                    {38, 9, "3.44E-7", -1, -1, "0", "false"},
                                    {38, 9, "14475.668159673", -1, -1, "14475", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 9, "2444829636929024.580347154", -1, -1, "", "true"},
                                    {38, 9, "-99189356508216744242088377671.675622560", -1, -1, "", "true"},
                                    {38, 9, "-99999999102808586000288935214.118863083", -1, -1, "", "true"},
                                    {38, 9, "28671362849804457174380188711.583393114", -1, -1, "", "true"},
                                    {38, 9, "281874889006240190610.522217699", -1, -1, "", "true"},
                                    {38, 9, "18642309270979807122410305156.542046247", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999506342", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999958872", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999967418011.466149020", -1, -1, "", "true"},
                                    {38, 9, "-99999999999971936317023137316.641382089", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999992.768220080", -1, -1, "", "true"},
                                    {38, 9, "-99999999999988092416241856573.884948116", -1, -1, "", "true"},
                                    {38, 9, "1592791929602169141686631410.696510725", -1, -1, "", "true"},
                                    {38, 9, "6896606463260303479.918029953", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToIntNormal) {
    CastTestCaseArray test_cases = {{38, 9, "1.42E-7", -1, -1, "0", "false"},
                                    {38, 9, "0.000028749", -1, -1, "0", "false"},
                                    {38, 9, "0.322258429", -1, -1, "0", "false"},
                                    {38, 9, "18.584432897", -1, -1, "18", "false"},
                                    {38, 9, "15862593.306883262", -1, -1, "15862593", "false"},
                                    {38, 9, "3474.800306645", -1, -1, "3474", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToBigIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 9, "-99999999999999999999999999695.766106100", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999998430.347367851", -1, -1, "", "true"},
                                    {38, 9, "-99999306373336916270951456507.741814648", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999954942372.922521802", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999754867913004659.926878716", -1, -1, "", "true"},
                                    {38, 9, "-99999999802174027519840119206.210223983", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999996081887905738.167072609", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999998516", -1, -1, "", "true"},
                                    {38, 9, "70141183460469231731687303715.884105728", -1, -1, "", "true"},
                                    {38, 9, "-99999999999974914872392239877.844157068", -1, -1, "", "true"},
                                    {38, 9, "-99960750109296900688611320833.157214071", -1, -1, "", "true"},
                                    {38, 9, "-99999999997793371864940085269.871081285", -1, -1, "", "true"},
                                    {38, 9, "-99999998759444238079118464948.239707046", -1, -1, "", "true"},
                                    {38, 9, "-99999999999999999999999999999.999999779", -1, -1, "", "true"},
                                    {38, 9, "-99999592054000615470281543619.667722858", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToBigIntNormal) {
    CastTestCaseArray test_cases = {{38, 9, "67.674799167", -1, -1, "67", "false"},
                                    {38, 9, "8850362668.063292300", -1, -1, "8850362668", "false"},
                                    {38, 9, "51672.676441759", -1, -1, "51672", "false"},
                                    {38, 9, "22242530646913.504831247", -1, -1, "22242530646913", "false"},
                                    {38, 9, "0.001111576", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s9ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 9, "-99999999999999999999999999999.971382664", -1, -1, "-99999999999999999999999999999", "false"},
            {38, 9, "745073943040458077459117391.089983975", -1, -1, "745073943040458077459117391", "false"},
            {38, 9, "-99994213274150500891666350899.051103047", -1, -1, "-99994213274150500891666350899", "false"},
            {38, 9, "293215.216245030", -1, -1, "293215", "false"},
            {38, 9, "68407.682633023", -1, -1, "68407", "false"},
            {38, 9, "1.417104681", -1, -1, "1", "false"},
            {38, 9, "28.292249767", -1, -1, "28", "false"},
            {38, 9, "1823981.422902648", -1, -1, "1823981", "false"},
            {38, 9, "-99993956577397127121795917813.946754502", -1, -1, "-99993956577397127121795917813", "false"},
            {38, 9, "3272774375272359246611.434931834", -1, -1, "3272774375272359246611", "false"},
            {38, 9, "-99999999999999999999999998432.708061215", -1, -1, "-99999999999999999999999998432", "false"},
            {38, 9, "38235935174756744409538626356.256039895", -1, -1, "38235935174756744409538626356", "false"},
            {38, 9, "9.080908026", -1, -1, "9", "false"},
            {38, 9, "773558082215318963525.194938640", -1, -1, "773558082215318963525", "false"},
            {38, 9, "-99999999997218661031699324965.893614590", -1, -1, "-99999999997218661031699324965", "false"},
            {38, 9, "-99999996450205237152129292567.812956017", -1, -1, "-99999996450205237152129292567", "false"},
            {38, 9, "-99999999999999999999999711846.265962776", -1, -1, "-99999999999999999999999711846", "false"},
            {38, 9, "-99999999999999984017088725240.598785608", -1, -1, "-99999999999999984017088725240", "false"},
            {38, 9, "132103721558.531315506", -1, -1, "132103721558", "false"},
            {38, 9, "2643797.850657169", -1, -1, "2643797", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToBooleanNormal) {
    CastTestCaseArray test_cases = {{38, 13, "0.0060099041825", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999999075077712799.6853853228337", -1, -1, "true", "false"},
                                    {38, 13, "5194224835.0469432704542", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999999999999576926.4865101352958", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999999999418504864.9514334502542", -1, -1, "true", "false"},
                                    {38, 13, "-9999987988596157227735894.0435367899363", -1, -1, "true", "false"},
                                    {38, 13, "12005385866152394062893.6774779936192", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999992373921215335.6679377897177", -1, -1, "true", "false"},
                                    {38, 13, "102203.1260227044128", -1, -1, "true", "false"},
                                    {38, 13, "-9999999991649123868076323.1955976451332", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999999999999999999.9999999996329", -1, -1, "true", "false"},
                                    {38, 13, "0.0026090720020", -1, -1, "true", "false"},
                                    {38, 13, "1.2057E-9", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999999999999619997.4257429421360", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999998106737418189.3072713526802", -1, -1, "true", "false"},
                                    {38, 13, "4601008.9765902257268", -1, -1, "true", "false"},
                                    {38, 13, "-9999999999999999999999999.9999999999998", -1, -1, "true", "false"},
                                    {38, 13, "961644525222595467.4824340584538", -1, -1, "true", "false"},
                                    {38, 13, "3.7238463394330", -1, -1, "true", "false"},
                                    {38, 13, "702582388627.2219558955530", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 13, "-9999999999999999999999993.3337644403408", -1, -1, "", "true"},
                                    {38, 13, "1361745856787664783.7179007016632", -1, -1, "", "true"},
                                    {38, 13, "-9999999542987570502902945.3295893538293", -1, -1, "", "true"},
                                    {38, 13, "343590536204933.6230721715167", -1, -1, "", "true"},
                                    {38, 13, "-9999831471797710638446385.6979292999762", -1, -1, "", "true"},
                                    {38, 13, "-9999999521408110520865695.0955321352246", -1, -1, "", "true"},
                                    {38, 13, "3277919877699440.3607060478254", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999950144694", -1, -1, "", "true"},
                                    {38, 13, "16848770325646031323.5512170483053", -1, -1, "", "true"},
                                    {38, 13, "51922409673.6882495143443", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999996.2897149718393", -1, -1, "", "true"},
                                    {38, 13, "-9997368037692537680047394.2928360351994", -1, -1, "", "true"},
                                    {38, 13, "1307094723805694379721580.8813730827034", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999122953212453.2957246806851", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToTinyIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 13, "0.0000244942188", -1, -1, "0", "false"}, {38, 13, "4.66689E-8", -1, -1, "0", "false"},
            {38, 13, "0E-13", -1, -1, "0", "false"},           {38, 13, "5E-13", -1, -1, "0", "false"},
            {38, 13, "8E-13", -1, -1, "0", "false"},           {38, 13, "1.818279E-7", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 13, "320415714217094287.1965995109768", -1, -1, "", "true"},
                                    {38, 13, "2105721938560942980471959.5066692863647", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999550807591", -1, -1, "", "true"},
                                    {38, 13, "-9943817488794600182497790.0939814173029", -1, -1, "", "true"},
                                    {38, 13, "-9999999999987589556839975.3595503608824", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999999982600", -1, -1, "", "true"},
                                    {38, 13, "-9999964020091282886474480.5129486768839", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999794505662", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999992219276738.6980643173995", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999890440.2760566466175", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999999999974", -1, -1, "", "true"},
                                    {38, 13, "7014118346046923173168730.3715884105728", -1, -1, "", "true"},
                                    {38, 13, "-9999994006538049535111687.5914903700338", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999762851.2186167947335", -1, -1, "", "true"},
                                    {38, 13, "196814.3435285573264", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999972959273.3046296116800", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{38, 13, "0.0000153982135", -1, -1, "0", "false"},
                                    {38, 13, "0.2152591244937", -1, -1, "0", "false"},
                                    {38, 13, "403.1310894256450", -1, -1, "403", "false"},
                                    {38, 13, "6.691292E-7", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 13, "-9999999999999999999999999.9999628570201", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999999999932", -1, -1, "", "true"},
                                    {38, 13, "7141169671577181.4314391937017", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999634660880", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999999999939", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999992931238.2230086170661", -1, -1, "", "true"},
                                    {38, 13, "25741988832343306678946.6088172262968", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999833677.6294768462913", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999969018288.1777121099695", -1, -1, "", "true"},
                                    {38, 13, "821811749984668.2692074323257", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToIntNormal) {
    CastTestCaseArray test_cases = {{38, 13, "0.0028572206685", -1, -1, "0", "false"},
                                    {38, 13, "68173.1535825795520", -1, -1, "68173", "false"},
                                    {38, 13, "6.470E-10", -1, -1, "0", "false"},
                                    {38, 13, "1168.1122228196560", -1, -1, "1168", "false"},
                                    {38, 13, "5.6464891010798", -1, -1, "5", "false"},
                                    {38, 13, "10307.9930355269771", -1, -1, "10307", "false"},
                                    {38, 13, "4.51520E-8", -1, -1, "0", "false"},
                                    {38, 13, "166863.2440991865002", -1, -1, "166863", "false"},
                                    {38, 13, "899.7170519534903", -1, -1, "899", "false"},
                                    {38, 13, "5288989.0334196268624", -1, -1, "5288989", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToBigIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 13, "-9999999999884100384740810.3438852021251", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999999999998", -1, -1, "", "true"},
                                    {38, 13, "-9994411530290565035629837.8270429801631", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999987641310.4901843495743", -1, -1, "", "true"},
                                    {38, 13, "-9999999900222141443016583.7418985395506", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999999999.9999999999932", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999998664739881.4483639424423", -1, -1, "", "true"},
                                    {38, 13, "825844818421613322597552.3510619900062", -1, -1, "", "true"},
                                    {38, 13, "-9999999998997879082006820.1149068216248", -1, -1, "", "true"},
                                    {38, 13, "-9999999999999999999993765.2763140795052", -1, -1, "", "true"},
                                    {38, 13, "-9976262438293289717054372.5303929918966", -1, -1, "", "true"},
                                    {38, 13, "-9999993510972886112039076.0568441207850", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToBigIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 13, "0.0061826876670", -1, -1, "0", "false"},
            {38, 13, "0.1564634883637", -1, -1, "0", "false"},
            {38, 13, "19407357283181406.0673735195204", -1, -1, "19407357283181406", "false"},
            {38, 13, "2.55E-11", -1, -1, "0", "false"},
            {38, 13, "1.619659E-7", -1, -1, "0", "false"},
            {38, 13, "25230483847.6997874046661", -1, -1, "25230483847", "false"},
            {38, 13, "101254688184293597.6256365147209", -1, -1, "101254688184293597", "false"},
            {38, 13, "699658836783295418.7035351169284", -1, -1, "699658836783295418", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s13ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 13, "-8733456262288748862705726.5472149987969", -1, -1, "-8733456262288748862705726", "false"},
            {38, 13, "0.0003687931398", -1, -1, "0", "false"},
            {38, 13, "63547838625288648868.8008774790644", -1, -1, "63547838625288648868", "false"},
            {38, 13, "-9999999999999999999999999.9999999972398", -1, -1, "-9999999999999999999999999", "false"},
            {38, 13, "-9999999999999931179215708.9712185252248", -1, -1, "-9999999999999931179215708", "false"},
            {38, 13, "-9999999999999999999999999.9998819394929", -1, -1, "-9999999999999999999999999", "false"},
            {38, 13, "-9999999999999999999999999.9999999997473", -1, -1, "-9999999999999999999999999", "false"},
            {38, 13, "-9999999999999999999999999.9999999999983", -1, -1, "-9999999999999999999999999", "false"},
            {38, 13, "237715758340.8417657324408", -1, -1, "237715758340", "false"},
            {38, 13, "0.0061511892030", -1, -1, "0", "false"},
            {38, 13, "2853.2667532362008", -1, -1, "2853", "false"},
            {38, 13, "-9999999625679341346440685.5972711124353", -1, -1, "-9999999625679341346440685", "false"},
            {38, 13, "-2985881653953076826831269.6284115894270", -1, -1, "-2985881653953076826831269", "false"},
            {38, 13, "-9999999999999534881180335.0175502082359", -1, -1, "-9999999999999534881180335", "false"},
            {38, 13, "-9999999999999999999999999.9998867514025", -1, -1, "-9999999999999999999999999", "false"},
            {38, 13, "3568450.4673235259138", -1, -1, "3568450", "false"},
            {38, 13, "1521962119328858551861245.0614069500174", -1, -1, "1521962119328858551861245", "false"},
            {38, 13, "0.0421035003633", -1, -1, "0", "false"},
            {38, 13, "5.66218E-8", -1, -1, "0", "false"},
            {38, 13, "-9999999999999999834098847.8012971719932", -1, -1, "-9999999999999999834098847", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToBooleanNormal) {
    CastTestCaseArray test_cases = {{38, 18, "1662318.240410368481485917", -1, -1, "true", "false"},
                                    {38, 18, "16254458067851343672.105965113685611399", -1, -1, "true", "false"},
                                    {38, 18, "-99999999999999999997.430248534384178614", -1, -1, "true", "false"},
                                    {38, 18, "10457419954294385.104420843171906499", -1, -1, "true", "false"},
                                    {38, 18, "-99924228276457736503.027487355052035923", -1, -1, "true", "false"},
                                    {38, 18, "3.95323032E-10", -1, -1, "true", "false"},
                                    {38, 18, "-99999999999999999999.999999997691659034", -1, -1, "true", "false"},
                                    {38, 18, "1.25408444E-10", -1, -1, "true", "false"},
                                    {38, 18, "664380343.472620712322029567", -1, -1, "true", "false"},
                                    {38, 18, "19.020552771416404880", -1, -1, "true", "false"},
                                    {38, 18, "127598437097533766.396197816848089392", -1, -1, "true", "false"},
                                    {38, 18, "55658781.502732552814593013", -1, -1, "true", "false"},
                                    {38, 18, "16354101.957653262847235350", -1, -1, "true", "false"},
                                    {38, 18, "-99999999999995306208.129392560937331175", -1, -1, "true", "false"},
                                    {38, 18, "439814.643965877640109256", -1, -1, "true", "false"},
                                    {38, 18, "-99999999999999999999.999874203933595042", -1, -1, "true", "false"},
                                    {38, 18, "0.004114234833758957", -1, -1, "true", "false"},
                                    {38, 18, "-99999781253879517977.502595112650014221", -1, -1, "true", "false"},
                                    {38, 18, "69391489007653701370.326991054995027550", -1, -1, "true", "false"},
                                    {38, 18, "-99999999999999999999.999999999892466384", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 18, "2007.142097721135182683", -1, -1, "", "true"},
                                    {38, 18, "1295.031873434241396640", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999952.503858999917662650", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999999974", -1, -1, "", "true"},
                                    {38, 18, "45697575503447.166615216113295033", -1, -1, "", "true"},
                                    {38, 18, "-99999996341610547306.913703190720972519", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999988995369", -1, -1, "", "true"},
                                    {38, 18, "2455611095504270780.365302145934940306", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999075524", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999998409", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999999879", -1, -1, "", "true"},
                                    {38, 18, "-91651616880858072532.310266742438912324", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999995729.696781434257936188", -1, -1, "", "true"},
                                    {38, 18, "3021687411998830237.300428310257162252", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{38, 18, "11.797408975657230933", -1, -1, "11", "false"},
                                    {38, 18, "2.293344103351805983", -1, -1, "2", "false"},
                                    {38, 18, "0.033039756080239133", -1, -1, "0", "false"},
                                    {38, 18, "1.63135E-13", -1, -1, "0", "false"},
                                    {38, 18, "4E-18", -1, -1, "0", "false"},
                                    {38, 18, "3.771856177E-9", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 18, "-99999999999999999999.999999466104356330", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999955.730008633694729290", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999860.329357665739543329", -1, -1, "", "true"},
                                    {38, 18, "60934964087003.968834909372377063", -1, -1, "", "true"},
                                    {38, 18, "112687410.476015705612934381", -1, -1, "", "true"},
                                    {38, 18, "3703098629246.051417880649033985", -1, -1, "", "true"},
                                    {38, 18, "-99999999999951205108.154132632244452359", -1, -1, "", "true"},
                                    {38, 18, "-96902637181640581300.936373131243516435", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999965830.755367816320076912", -1, -1, "", "true"},
                                    {38, 18, "-36701654983812932410.497058426528531509", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.997222040110775885", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999985507218", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.097748203721825876", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999548759", -1, -1, "", "true"},
                                    {38, 18, "-99999999890228803730.043101743117905595", -1, -1, "", "true"},
                                    {38, 18, "79500169511.101434192987169854", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{38, 18, "65.603004134233123238", -1, -1, "65", "false"},
                                    {38, 18, "4E-18", -1, -1, "0", "false"},
                                    {38, 18, "1.59215686603E-7", -1, -1, "0", "false"},
                                    {38, 18, "1.4E-17", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 18, "-99839204680309502567.185269069372611630", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999937213491912", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999757412", -1, -1, "", "true"},
                                    {38, 18, "-99999999877032295279.255316417797712804", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999850636.579835395732972550", -1, -1, "", "true"},
                                    {38, 18, "8402120549514387.975640919173743196", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999995889284", -1, -1, "", "true"},
                                    {38, 18, "4029510104635764844.986802808546316698", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999989024727021571", -1, -1, "", "true"},
                                    {38, 18, "-99998449661589685433.347849909376379098", -1, -1, "", "true"},
                                    {38, 18, "-99999999999998902598.277120481266412602", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToIntNormal) {
    CastTestCaseArray test_cases = {{38, 18, "3.1E-17", -1, -1, "0", "false"},
                                    {38, 18, "2.103E-15", -1, -1, "0", "false"},
                                    {38, 18, "0.000485796696596059", -1, -1, "0", "false"},
                                    {38, 18, "2064730.776184276007624386", -1, -1, "2064730", "false"},
                                    {38, 18, "84.865773354126210525", -1, -1, "84", "false"},
                                    {38, 18, "3.679051E-12", -1, -1, "0", "false"},
                                    {38, 18, "0.003565603967636740", -1, -1, "0", "false"},
                                    {38, 18, "500253.196296600267915731", -1, -1, "500253", "false"},
                                    {38, 18, "0.000007662920414870", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToBigIntAbnormal) {
    CastTestCaseArray test_cases = {{38, 18, "-99999999999999999999.999999999999986973", -1, -1, "", "true"},
                                    {38, 18, "-99399002228885079228.981912764737909016", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999300.286377039062784360", -1, -1, "", "true"},
                                    {38, 18, "33342142725694071218.148230420497182206", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999999.999999999999999996", -1, -1, "", "true"},
                                    {38, 18, "-99681245081706240221.387250731799161164", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999999870.755678830834651361", -1, -1, "", "true"},
                                    {38, 18, "-99999999999999998108.341421023749933420", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToBigIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 18, "126.113881021197308078", -1, -1, "126", "false"},
            {38, 18, "4612694774289330.174202949180908626", -1, -1, "4612694774289330", "false"},
            {38, 18, "0.000001317126534552", -1, -1, "0", "false"},
            {38, 18, "7.4250E-14", -1, -1, "0", "false"},
            {38, 18, "4.7872E-14", -1, -1, "0", "false"},
            {38, 18, "278.176272156599810338", -1, -1, "278", "false"},
            {38, 18, "0.248618615860981653", -1, -1, "0", "false"},
            {38, 18, "0.016216448511543915", -1, -1, "0", "false"},
            {38, 18, "6.195605325771564617", -1, -1, "6", "false"},
            {38, 18, "1.08E-16", -1, -1, "0", "false"},
            {38, 18, "0E-18", -1, -1, "0", "false"},
            {38, 18, "0.000014943793783939", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p38s18ToLargeIntNormal) {
    CastTestCaseArray test_cases = {
            {38, 18, "-32616214688032419206.486374370392852536", -1, -1, "-32616214688032419206", "false"},
            {38, 18, "8.7353E-14", -1, -1, "0", "false"},
            {38, 18, "-99999999999999999999.999999996181439726", -1, -1, "-99999999999999999999", "false"},
            {38, 18, "-99999999999999999998.674343102485472865", -1, -1, "-99999999999999999998", "false"},
            {38, 18, "-99999985213503847943.260724790619117067", -1, -1, "-99999985213503847943", "false"},
            {38, 18, "-99999999999999999746.153066165741032094", -1, -1, "-99999999999999999746", "false"},
            {38, 18, "3.33885E-13", -1, -1, "0", "false"},
            {38, 18, "0.000002221925784805", -1, -1, "0", "false"},
            {38, 18, "894057136531864.643880311884414772", -1, -1, "894057136531864", "false"},
            {38, 18, "-99999999999999999999.302094236862342294", -1, -1, "-99999999999999999999", "false"},
            {38, 18, "126359.067352239977331563", -1, -1, "126359", "false"},
            {38, 18, "5.295006737345742281", -1, -1, "5", "false"},
            {38, 18, "-99999999999999999903.458427019726993629", -1, -1, "-99999999999999999903", "false"},
            {38, 18, "64809583448.092506189650501798", -1, -1, "64809583448", "false"},
            {38, 18, "92.303188133481818955", -1, -1, "92", "false"},
            {38, 18, "-99999999999999999999.999999999999999927", -1, -1, "-99999999999999999999", "false"},
            {38, 18, "-99963737894508903914.752572213729788821", -1, -1, "-99963737894508903914", "false"},
            {38, 18, "-99999999999999999999.718052095251938943", -1, -1, "-99999999999999999999", "false"},
            {38, 18, "274945712521829.458862660761546748", -1, -1, "274945712521829", "false"},
            {38, 18, "-99999999960925945163.764214456471355166", -1, -1, "-99999999960925945163", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_LARGEINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToBooleanNormal) {
    CastTestCaseArray test_cases = {{35, 30, "-99999.999999999999999999999999259749", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999896141969772254148", -1, -1, "true", "false"},
                                    {35, 30, "79393.643646378595080312618284929465", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999700617644490478356310968", -1, -1, "true", "false"},
                                    {35, 30, "0.935488688208659010274586871194", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999980389063573122437555527", -1, -1, "true", "false"},
                                    {35, 30, "3.693672434433211853E-12", -1, -1, "true", "false"},
                                    {35, 30, "41183.460469231731687303715884107428", -1, -1, "true", "false"},
                                    {35, 30, "99129.955064378830452517168274083191", -1, -1, "true", "false"},
                                    {35, 30, "1.647380487E-21", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999999999999999972183", -1, -1, "true", "false"},
                                    {35, 30, "0.000055192744360893141882495052", -1, -1, "true", "false"},
                                    {35, 30, "43.871732941731590323756915380853", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999999699198363676649", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999999959433806792539", -1, -1, "true", "false"},
                                    {35, 30, "-99999.996124671479627405174451847012", -1, -1, "true", "false"},
                                    {35, 30, "2.708841945361724687113E-9", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999998465003346793330", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999999999996720437604", -1, -1, "true", "false"},
                                    {35, 30, "-99999.999999999999999999999999731190", -1, -1, "true", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BOOLEAN>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToTinyIntAbnormal) {
    CastTestCaseArray test_cases = {{35, 30, "34629.858316951507002687058663746799", -1, -1, "", "true"},
                                    {35, 30, "41183.460469231731687303715884107428", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999121998067468019", -1, -1, "", "true"},
                                    {35, 30, "74912.426008530118110515974117560618", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999990126267847847000660", -1, -1, "", "true"},
                                    {35, 30, "-99999.999998482850274010943175755873", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999228872822242976660256", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999999999977682206", -1, -1, "", "true"},
                                    {35, 30, "-95285.252249896993340824054484525772", -1, -1, "", "true"},
                                    {35, 30, "-99163.968671000213709889840243923428", -1, -1, "", "true"},
                                    {35, 30, "956.675260027652655012074928603421", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999999740706178403", -1, -1, "", "true"},
                                    {35, 30, "-99999.997694295600180378444011558709", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999999991606477515", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToTinyIntNormal) {
    CastTestCaseArray test_cases = {{35, 30, "7.513596862976318229E-12", -1, -1, "0", "false"},
                                    {35, 30, "9.5603786308592772E-14", -1, -1, "0", "false"},
                                    {35, 30, "0.000006191105338139865655570641", -1, -1, "0", "false"},
                                    {35, 30, "0.271258184425695197484011175651", -1, -1, "0", "false"},
                                    {35, 30, "3.516253113139113551E-12", -1, -1, "0", "false"},
                                    {35, 30, "11.379317982244273399850333391489", -1, -1, "11", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_TINYINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToSmallIntAbnormal) {
    CastTestCaseArray test_cases = {{35, 30, "-99999.999987863709855934840515432147", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999986980193703639", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999999999999999233", -1, -1, "", "true"},
                                    {35, 30, "-99999.731482870696159603017247195821", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999987815171070130232657", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999999999999886723", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999999999999961393", -1, -1, "", "true"},
                                    {35, 30, "-99999.990644528725307247815376194059", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999804989713761637", -1, -1, "", "true"},
                                    {35, 30, "-99999.997671487209662495874335820429", -1, -1, "", "true"},
                                    {35, 30, "-99999.999999999999999983438255026811", -1, -1, "", "true"}};
    test_cast_all_fail<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToSmallIntNormal) {
    CastTestCaseArray test_cases = {{35, 30, "6.896495411176E-18", -1, -1, "0", "false"},
                                    {35, 30, "4.5642921757E-20", -1, -1, "0", "false"},
                                    {35, 30, "2.52060E-25", -1, -1, "0", "false"},
                                    {35, 30, "3.798420E-24", -1, -1, "0", "false"},
                                    {35, 30, "1.41965905786879E-16", -1, -1, "0", "false"},
                                    {35, 30, "2.46123006545707114686136E-7", -1, -1, "0", "false"},
                                    {35, 30, "0.572747461294498969978631266337", -1, -1, "0", "false"},
                                    {35, 30, "0.000001148661844081626881977030", -1, -1, "0", "false"},
                                    {35, 30, "6.2664927250581100482434E-8", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_SMALLINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToIntNormal) {
    CastTestCaseArray test_cases = {{35, 30, "-9081.624515877641787522938824314058", -1, -1, "-9081", "false"},
                                    {35, 30, "-99999.999999999999999999999999999998", -1, -1, "-99999", "false"},
                                    {35, 30, "1.5703472226767648E-14", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999486832796910875409314021954", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999708798635120919731454", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999999999999999996521501", -1, -1, "-99999", "false"},
                                    {35, 30, "9.4958329E-23", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999925527201628", -1, -1, "-99999", "false"},
                                    {35, 30, "-47115.171243334990325248300535814707", -1, -1, "-47115", "false"},
                                    {35, 30, "5.61652E-25", -1, -1, "0", "false"},
                                    {35, 30, "34725.598786223904782074216952663671", -1, -1, "34725", "false"},
                                    {35, 30, "-99999.999999999999890110788052138791", -1, -1, "-99999", "false"},
                                    {35, 30, "-99917.908514117550601495190041500521", -1, -1, "-99917", "false"},
                                    {35, 30, "5.119281970364E-18", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999998882982676552047", -1, -1, "-99999", "false"},
                                    {35, 30, "1.73447562896711200604E-10", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999999999999993", -1, -1, "-99999", "false"},
                                    {35, 30, "0.000254506323734907643544099246", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999999999999500", -1, -1, "-99999", "false"},
                                    {35, 30, "1978.634809862342265658642324798261", -1, -1, "1978", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_INT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToBigIntNormal) {
    CastTestCaseArray test_cases = {{35, 30, "-99999.999999999999999999999999999946", -1, -1, "-99999", "false"},
                                    {35, 30, "29472.531335250180474796517807203563", -1, -1, "29472", "false"},
                                    {35, 30, "-99999.999999999999393882002473717293", -1, -1, "-99999", "false"},
                                    {35, 30, "5.4956042729026059470E-11", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999999999549406", -1, -1, "-99999", "false"},
                                    {35, 30, "4.21322E-25", -1, -1, "0", "false"},
                                    {35, 30, "4.508154108E-21", -1, -1, "0", "false"},
                                    {35, 30, "1E-30", -1, -1, "0", "false"},
                                    {35, 30, "3.960090844784E-18", -1, -1, "0", "false"},
                                    {35, 30, "-99589.302487439810947879209265926333", -1, -1, "-99589", "false"},
                                    {35, 30, "4.114871801E-21", -1, -1, "0", "false"},
                                    {35, 30, "2.28E-28", -1, -1, "0", "false"},
                                    {35, 30, "0.000001012417622590721306835381", -1, -1, "0", "false"},
                                    {35, 30, "9.9595499465184330876E-11", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999999999999986", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999999999999991552372097", -1, -1, "-99999", "false"},
                                    {35, 30, "1.4674E-26", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999999999999995", -1, -1, "-99999", "false"},
                                    {35, 30, "3.989516356534E-18", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999997898880853714", -1, -1, "-99999", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_BIGINT>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromDecimal128p35s30ToLargeIntNormal) {
    CastTestCaseArray test_cases = {{35, 30, "7.692042384E-21", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999928498575340880311", -1, -1, "-99999", "false"},
                                    {35, 30, "6.4955488400302870771E-11", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999999456841194", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999999993911379158512202", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.960601030402382662090307833174", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999998151580494605621761429", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999985540041417047829397147", -1, -1, "-99999", "false"},
                                    {35, 30, "1.2653479E-23", -1, -1, "0", "false"},
                                    {35, 30, "28357.876236946634025673157943095095", -1, -1, "28357", "false"},
                                    {35, 30, "0.009619108004301672012649578041", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999998875850403643361682", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999999999999999999999966", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999999999248906502878607", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999999999999999946456960108", -1, -1, "-99999", "false"},
                                    {35, 30, "1.91415164174571790017024E-7", -1, -1, "0", "false"},
                                    {35, 30, "-99999.999999999999999999767760368331", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999999993274729348185262817533", -1, -1, "-99999", "false"},
                                    {35, 30, "-99999.999918234138678497038055437288", -1, -1, "-99999", "false"},
                                    {35, 30, "1.195E-27", -1, -1, "0", "false"}};
    test_cast_all<TYPE_DECIMAL128, TYPE_LARGEINT>(test_cases);
}
PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 9, 0, "0", "false"}, {-1, -1, "true", 9, 0, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 9, 2, "1", "false"}, {-1, -1, "false", 9, 2, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 9, 9, "0", "false"}, {-1, -1, "true", 9, 9, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 7, 4, "0", "false"}, {-1, -1, "true", 7, 4, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 18, 0, "0", "false"}, {-1, -1, "true", 18, 0, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 18, 2, "1", "false"}, {-1, -1, "false", 18, 2, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 18, 9, "1", "false"}, {-1, -1, "false", 18, 9, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 18, 18, "1", "false"}, {-1, -1, "false", 18, 18, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 15, 13, "1", "false"}, {-1, -1, "false", 15, 13, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 38, 0, "0", "false"}, {-1, -1, "true", 38, 0, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 38, 2, "1", "false"}, {-1, -1, "false", 38, 2, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 38, 9, "0", "false"}, {-1, -1, "true", 38, 9, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "false", 38, 13, "0", "false"}, {-1, -1, "true", 38, 13, "1", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 38, 18, "1", "false"}, {-1, -1, "false", 38, 18, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBooleanToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "true", 35, 30, "1", "false"}, {-1, -1, "false", 35, 30, "0", "false"}};
    test_cast_all<TYPE_BOOLEAN, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-77", 9, 0, "-77", "false"}, {-1, -1, "-6", 9, 0, "-6", "false"},
                                    {-1, -1, "91", 9, 0, "91", "false"},   {-1, -1, "-5", 9, 0, "-5", "false"},
                                    {-1, -1, "107", 9, 0, "107", "false"}, {-1, -1, "-1", 9, 0, "-1", "false"},
                                    {-1, -1, "7", 9, 0, "7", "false"},     {-1, -1, "27", 9, 0, "27", "false"},
                                    {-1, -1, "54", 9, 0, "54", "false"},   {-1, -1, "-3", 9, 0, "-3", "false"},
                                    {-1, -1, "-4", 9, 0, "-4", "false"},   {-1, -1, "28", 9, 0, "28", "false"},
                                    {-1, -1, "-87", 9, 0, "-87", "false"}, {-1, -1, "46", 9, 0, "46", "false"},
                                    {-1, -1, "12", 9, 0, "12", "false"},   {-1, -1, "20", 9, 0, "20", "false"},
                                    {-1, -1, "0", 9, 0, "0", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-88", 9, 2, "-88", "false"},   {-1, -1, "127", 9, 2, "127", "false"},
                                    {-1, -1, "24", 9, 2, "24", "false"},     {-1, -1, "2", 9, 2, "2", "false"},
                                    {-1, -1, "12", 9, 2, "12", "false"},     {-1, -1, "75", 9, 2, "75", "false"},
                                    {-1, -1, "-4", 9, 2, "-4", "false"},     {-1, -1, "-7", 9, 2, "-7", "false"},
                                    {-1, -1, "-1", 9, 2, "-1", "false"},     {-1, -1, "-23", 9, 2, "-23", "false"},
                                    {-1, -1, "-112", 9, 2, "-112", "false"}, {-1, -1, "39", 9, 2, "39", "false"},
                                    {-1, -1, "1", 9, 2, "1", "false"},       {-1, -1, "-128", 9, 2, "-128", "false"},
                                    {-1, -1, "14", 9, 2, "14", "false"},     {-1, -1, "-2", 9, 2, "-2", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-39", 9, 9, "", "true"}, {-1, -1, "9", 9, 9, "", "true"},   {-1, -1, "-12", 9, 9, "", "true"},
            {-1, -1, "3", 9, 9, "", "true"},   {-1, -1, "113", 9, 9, "", "true"}, {-1, -1, "-4", 9, 9, "", "true"},
            {-1, -1, "120", 9, 9, "", "true"}, {-1, -1, "5", 9, 9, "", "true"},   {-1, -1, "15", 9, 9, "", "true"},
            {-1, -1, "-29", 9, 9, "", "true"}, {-1, -1, "62", 9, 9, "", "true"},  {-1, -1, "-128", 9, 9, "", "true"},
            {-1, -1, "127", 9, 9, "", "true"}};
    test_cast_all_fail<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-2", 9, 9, "-2", "false"},
                                    {-1, -1, "1", 9, 9, "1", "false"},
                                    {-1, -1, "-1", 9, 9, "-1", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-119", 7, 4, "-119", "false"}, {-1, -1, "-1", 7, 4, "-1", "false"},
                                    {-1, -1, "-51", 7, 4, "-51", "false"},   {-1, -1, "-13", 7, 4, "-13", "false"},
                                    {-1, -1, "29", 7, 4, "29", "false"},     {-1, -1, "-128", 7, 4, "-128", "false"},
                                    {-1, -1, "-8", 7, 4, "-8", "false"},     {-1, -1, "-126", 7, 4, "-126", "false"},
                                    {-1, -1, "71", 7, 4, "71", "false"},     {-1, -1, "-81", 7, 4, "-81", "false"},
                                    {-1, -1, "3", 7, 4, "3", "false"},       {-1, -1, "-63", 7, 4, "-63", "false"},
                                    {-1, -1, "30", 7, 4, "30", "false"},     {-1, -1, "102", 7, 4, "102", "false"},
                                    {-1, -1, "-4", 7, 4, "-4", "false"},     {-1, -1, "51", 7, 4, "51", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-3", 18, 0, "-3", "false"},    {-1, -1, "17", 18, 0, "17", "false"},
                                    {-1, -1, "-2", 18, 0, "-2", "false"},    {-1, -1, "-4", 18, 0, "-4", "false"},
                                    {-1, -1, "127", 18, 0, "127", "false"},  {-1, -1, "42", 18, 0, "42", "false"},
                                    {-1, -1, "-35", 18, 0, "-35", "false"},  {-1, -1, "-1", 18, 0, "-1", "false"},
                                    {-1, -1, "5", 18, 0, "5", "false"},      {-1, -1, "-29", 18, 0, "-29", "false"},
                                    {-1, -1, "1", 18, 0, "1", "false"},      {-1, -1, "-9", 18, 0, "-9", "false"},
                                    {-1, -1, "8", 18, 0, "8", "false"},      {-1, -1, "11", 18, 0, "11", "false"},
                                    {-1, -1, "-128", 18, 0, "-128", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-29", 18, 2, "-29", "false"}, {-1, -1, "4", 18, 2, "4", "false"},
                                    {-1, -1, "-16", 18, 2, "-16", "false"}, {-1, -1, "127", 18, 2, "127", "false"},
                                    {-1, -1, "1", 18, 2, "1", "false"},     {-1, -1, "-43", 18, 2, "-43", "false"},
                                    {-1, -1, "-1", 18, 2, "-1", "false"},   {-1, -1, "0", 18, 2, "0", "false"},
                                    {-1, -1, "-4", 18, 2, "-4", "false"},   {-1, -1, "-113", 18, 2, "-113", "false"},
                                    {-1, -1, "-6", 18, 2, "-6", "false"},   {-1, -1, "-21", 18, 2, "-21", "false"},
                                    {-1, -1, "6", 18, 2, "6", "false"},     {-1, -1, "105", 18, 2, "105", "false"},
                                    {-1, -1, "-2", 18, 2, "-2", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-8", 18, 9, "-8", "false"},     {-1, -1, "-31", 18, 9, "-31", "false"},
                                    {-1, -1, "7", 18, 9, "7", "false"},       {-1, -1, "76", 18, 9, "76", "false"},
                                    {-1, -1, "-128", 18, 9, "-128", "false"}, {-1, -1, "-1", 18, 9, "-1", "false"},
                                    {-1, -1, "-107", 18, 9, "-107", "false"}, {-1, -1, "3", 18, 9, "3", "false"},
                                    {-1, -1, "127", 18, 9, "127", "false"},   {-1, -1, "48", 18, 9, "48", "false"},
                                    {-1, -1, "-3", 18, 9, "-3", "false"},     {-1, -1, "2", 18, 9, "2", "false"},
                                    {-1, -1, "-18", 18, 9, "-18", "false"},   {-1, -1, "26", 18, 9, "26", "false"},
                                    {-1, -1, "15", 18, 9, "15", "false"},     {-1, -1, "-27", 18, 9, "-27", "false"},
                                    {-1, -1, "-6", 18, 9, "-6", "false"},     {-1, -1, "-40", 18, 9, "-40", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-36", 18, 18, "", "true"}, {-1, -1, "127", 18, 18, "", "true"},
                                    {-1, -1, "24", 18, 18, "", "true"},  {-1, -1, "-18", 18, 18, "", "true"},
                                    {-1, -1, "44", 18, 18, "", "true"},  {-1, -1, "-24", 18, 18, "", "true"},
                                    {-1, -1, "10", 18, 18, "", "true"},  {-1, -1, "-21", 18, 18, "", "true"},
                                    {-1, -1, "-31", 18, 18, "", "true"}};
    test_cast_all_fail<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "3", 18, 18, "3", "false"},   {-1, -1, "1", 18, 18, "1", "false"},
                                    {-1, -1, "-1", 18, 18, "-1", "false"}, {-1, -1, "0", 18, 18, "0", "false"},
                                    {-1, -1, "-6", 18, 18, "-6", "false"}, {-1, -1, "-4", 18, 18, "-4", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "0", 15, 13, "0", "false"},     {-1, -1, "1", 15, 13, "1", "false"},
                                    {-1, -1, "18", 15, 13, "18", "false"},   {-1, -1, "-76", 15, 13, "-76", "false"},
                                    {-1, -1, "42", 15, 13, "42", "false"},   {-1, -1, "7", 15, 13, "7", "false"},
                                    {-1, -1, "-85", 15, 13, "-85", "false"}, {-1, -1, "127", 15, 13, "127", "false"},
                                    {-1, -1, "-1", 15, 13, "-1", "false"},   {-1, -1, "-15", 15, 13, "-15", "false"},
                                    {-1, -1, "-10", 15, 13, "-10", "false"}, {-1, -1, "-3", 15, 13, "-3", "false"},
                                    {-1, -1, "6", 15, 13, "6", "false"},     {-1, -1, "-8", 15, 13, "-8", "false"},
                                    {-1, -1, "-2", 15, 13, "-2", "false"},   {-1, -1, "-128", 15, 13, "-128", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-62", 38, 0, "-62", "false"}, {-1, -1, "-6", 38, 0, "-6", "false"},
                                    {-1, -1, "-2", 38, 0, "-2", "false"},   {-1, -1, "-50", 38, 0, "-50", "false"},
                                    {-1, -1, "1", 38, 0, "1", "false"},     {-1, -1, "-4", 38, 0, "-4", "false"},
                                    {-1, -1, "-59", 38, 0, "-59", "false"}, {-1, -1, "-23", 38, 0, "-23", "false"},
                                    {-1, -1, "127", 38, 0, "127", "false"}, {-1, -1, "-3", 38, 0, "-3", "false"},
                                    {-1, -1, "99", 38, 0, "99", "false"},   {-1, -1, "-1", 38, 0, "-1", "false"},
                                    {-1, -1, "-30", 38, 0, "-30", "false"}, {-1, -1, "5", 38, 0, "5", "false"},
                                    {-1, -1, "3", 38, 0, "3", "false"},     {-1, -1, "22", 38, 0, "22", "false"},
                                    {-1, -1, "-92", 38, 0, "-92", "false"}, {-1, -1, "-37", 38, 0, "-37", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "5", 38, 2, "5", "false"},       {-1, -1, "1", 38, 2, "1", "false"},
                                    {-1, -1, "-107", 38, 2, "-107", "false"}, {-1, -1, "-2", 38, 2, "-2", "false"},
                                    {-1, -1, "27", 38, 2, "27", "false"},     {-1, -1, "127", 38, 2, "127", "false"},
                                    {-1, -1, "-15", 38, 2, "-15", "false"},   {-1, -1, "7", 38, 2, "7", "false"},
                                    {-1, -1, "-26", 38, 2, "-26", "false"},   {-1, -1, "3", 38, 2, "3", "false"},
                                    {-1, -1, "-61", 38, 2, "-61", "false"},   {-1, -1, "-59", 38, 2, "-59", "false"},
                                    {-1, -1, "-73", 38, 2, "-73", "false"},   {-1, -1, "-27", 38, 2, "-27", "false"},
                                    {-1, -1, "11", 38, 2, "11", "false"},     {-1, -1, "-8", 38, 2, "-8", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-55", 38, 9, "-55", "false"},   {-1, -1, "41", 38, 9, "41", "false"},
                                    {-1, -1, "127", 38, 9, "127", "false"},   {-1, -1, "-17", 38, 9, "-17", "false"},
                                    {-1, -1, "0", 38, 9, "0", "false"},       {-1, -1, "22", 38, 9, "22", "false"},
                                    {-1, -1, "-128", 38, 9, "-128", "false"}, {-1, -1, "6", 38, 9, "6", "false"},
                                    {-1, -1, "1", 38, 9, "1", "false"},       {-1, -1, "23", 38, 9, "23", "false"},
                                    {-1, -1, "-3", 38, 9, "-3", "false"},     {-1, -1, "-86", 38, 9, "-86", "false"},
                                    {-1, -1, "-20", 38, 9, "-20", "false"},   {-1, -1, "-5", 38, 9, "-5", "false"},
                                    {-1, -1, "-25", 38, 9, "-25", "false"},   {-1, -1, "57", 38, 9, "57", "false"},
                                    {-1, -1, "-1", 38, 9, "-1", "false"},     {-1, -1, "55", 38, 9, "55", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-9", 38, 13, "-9", "false"},   {-1, -1, "-128", 38, 13, "-128", "false"},
                                    {-1, -1, "0", 38, 13, "0", "false"},     {-1, -1, "-72", 38, 13, "-72", "false"},
                                    {-1, -1, "-53", 38, 13, "-53", "false"}, {-1, -1, "7", 38, 13, "7", "false"},
                                    {-1, -1, "-7", 38, 13, "-7", "false"},   {-1, -1, "36", 38, 13, "36", "false"},
                                    {-1, -1, "2", 38, 13, "2", "false"},     {-1, -1, "1", 38, 13, "1", "false"},
                                    {-1, -1, "-82", 38, 13, "-82", "false"}, {-1, -1, "-10", 38, 13, "-10", "false"},
                                    {-1, -1, "-50", 38, 13, "-50", "false"}, {-1, -1, "-11", 38, 13, "-11", "false"},
                                    {-1, -1, "-1", 38, 13, "-1", "false"},   {-1, -1, "-57", 38, 13, "-57", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "127", 38, 18, "127", "false"}, {-1, -1, "-2", 38, 18, "-2", "false"},
                                    {-1, -1, "1", 38, 18, "1", "false"},     {-1, -1, "17", 38, 18, "17", "false"},
                                    {-1, -1, "93", 38, 18, "93", "false"},   {-1, -1, "-14", 38, 18, "-14", "false"},
                                    {-1, -1, "-1", 38, 18, "-1", "false"},   {-1, -1, "-128", 38, 18, "-128", "false"},
                                    {-1, -1, "22", 38, 18, "22", "false"},   {-1, -1, "-3", 38, 18, "-3", "false"},
                                    {-1, -1, "-69", 38, 18, "-69", "false"}, {-1, -1, "-6", 38, 18, "-6", "false"},
                                    {-1, -1, "-9", 38, 18, "-9", "false"},   {-1, -1, "4", 38, 18, "4", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromTinyIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "0", 35, 30, "0", "false"},       {-1, -1, "94", 35, 30, "94", "false"},
                                    {-1, -1, "-2", 35, 30, "-2", "false"},     {-1, -1, "127", 35, 30, "127", "false"},
                                    {-1, -1, "-64", 35, 30, "-64", "false"},   {-1, -1, "-31", 35, 30, "-31", "false"},
                                    {-1, -1, "-13", 35, 30, "-13", "false"},   {-1, -1, "14", 35, 30, "14", "false"},
                                    {-1, -1, "29", 35, 30, "29", "false"},     {-1, -1, "-38", 35, 30, "-38", "false"},
                                    {-1, -1, "-1", 35, 30, "-1", "false"},     {-1, -1, "46", 35, 30, "46", "false"},
                                    {-1, -1, "-128", 35, 30, "-128", "false"}, {-1, -1, "-4", 35, 30, "-4", "false"},
                                    {-1, -1, "-49", 35, 30, "-49", "false"}};
    test_cast_all<TYPE_TINYINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "110", 9, 0, "110", "false"},       {-1, -1, "6", 9, 0, "6", "false"},
            {-1, -1, "-5713", 9, 0, "-5713", "false"},   {-1, -1, "-1", 9, 0, "-1", "false"},
            {-1, -1, "566", 9, 0, "566", "false"},       {-1, -1, "-7324", 9, 0, "-7324", "false"},
            {-1, -1, "-32768", 9, 0, "-32768", "false"}, {-1, -1, "7911", 9, 0, "7911", "false"},
            {-1, -1, "-14611", 9, 0, "-14611", "false"}, {-1, -1, "14", 9, 0, "14", "false"},
            {-1, -1, "21068", 9, 0, "21068", "false"},   {-1, -1, "-783", 9, 0, "-783", "false"},
            {-1, -1, "7279", 9, 0, "7279", "false"},     {-1, -1, "-16", 9, 0, "-16", "false"},
            {-1, -1, "-27", 9, 0, "-27", "false"},       {-1, -1, "2", 9, 0, "2", "false"},
            {-1, -1, "-11119", 9, 0, "-11119", "false"}, {-1, -1, "-5", 9, 0, "-5", "false"},
            {-1, -1, "-3078", 9, 0, "-3078", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-847", 9, 2, "-847", "false"},   {-1, -1, "-342", 9, 2, "-342", "false"},
            {-1, -1, "121", 9, 2, "121", "false"},     {-1, -1, "-28", 9, 2, "-28", "false"},
            {-1, -1, "5898", 9, 2, "5898", "false"},   {-1, -1, "3", 9, 2, "3", "false"},
            {-1, -1, "-7451", 9, 2, "-7451", "false"}, {-1, -1, "-544", 9, 2, "-544", "false"},
            {-1, -1, "687", 9, 2, "687", "false"},     {-1, -1, "30", 9, 2, "30", "false"},
            {-1, -1, "8045", 9, 2, "8045", "false"},   {-1, -1, "-190", 9, 2, "-190", "false"},
            {-1, -1, "-6574", 9, 2, "-6574", "false"}, {-1, -1, "-22447", 9, 2, "-22447", "false"},
            {-1, -1, "-2", 9, 2, "-2", "false"},       {-1, -1, "32767", 9, 2, "32767", "false"},
            {-1, -1, "-425", 9, 2, "-425", "false"},   {-1, -1, "-2107", 9, 2, "-2107", "false"},
            {-1, -1, "-89", 9, 2, "-89", "false"},     {-1, -1, "24415", 9, 2, "24415", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "236", 9, 9, "", "true"},   {-1, -1, "-56", 9, 9, "", "true"},
                                    {-1, -1, "-329", 9, 9, "", "true"},  {-1, -1, "5437", 9, 9, "", "true"},
                                    {-1, -1, "153", 9, 9, "", "true"},   {-1, -1, "33", 9, 9, "", "true"},
                                    {-1, -1, "224", 9, 9, "", "true"},   {-1, -1, "30758", 9, 9, "", "true"},
                                    {-1, -1, "993", 9, 9, "", "true"},   {-1, -1, "-15277", 9, 9, "", "true"},
                                    {-1, -1, "4", 9, 9, "", "true"},     {-1, -1, "-119", 9, 9, "", "true"},
                                    {-1, -1, "3", 9, 9, "", "true"},     {-1, -1, "268", 9, 9, "", "true"},
                                    {-1, -1, "12971", 9, 9, "", "true"}, {-1, -1, "3866", 9, 9, "", "true"},
                                    {-1, -1, "-30", 9, 9, "", "true"},   {-1, -1, "-10", 9, 9, "", "true"}};
    test_cast_all_fail<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-2", 9, 9, "-2", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "2", 7, 4, "2", "false"},         {-1, -1, "-167", 7, 4, "-167", "false"},
            {-1, -1, "72", 7, 4, "72", "false"},       {-1, -1, "-136", 7, 4, "-136", "false"},
            {-1, -1, "-7", 7, 4, "-7", "false"},       {-1, -1, "-7564", 7, 4, "-7564", "false"},
            {-1, -1, "0", 7, 4, "0", "false"},         {-1, -1, "-344", 7, 4, "-344", "false"},
            {-1, -1, "-1", 7, 4, "-1", "false"},       {-1, -1, "424", 7, 4, "424", "false"},
            {-1, -1, "329", 7, 4, "329", "false"},     {-1, -1, "3205", 7, 4, "3205", "false"},
            {-1, -1, "-774", 7, 4, "-774", "false"},   {-1, -1, "-31629", 7, 4, "-31629", "false"},
            {-1, -1, "15", 7, 4, "15", "false"},       {-1, -1, "-572", 7, 4, "-572", "false"},
            {-1, -1, "16333", 7, 4, "16333", "false"}, {-1, -1, "-16109", 7, 4, "-16109", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "875", 18, 0, "875", "false"},     {-1, -1, "1", 18, 0, "1", "false"},
            {-1, -1, "-30", 18, 0, "-30", "false"},     {-1, -1, "2523", 18, 0, "2523", "false"},
            {-1, -1, "-862", 18, 0, "-862", "false"},   {-1, -1, "-32", 18, 0, "-32", "false"},
            {-1, -1, "-5", 18, 0, "-5", "false"},       {-1, -1, "-3", 18, 0, "-3", "false"},
            {-1, -1, "-4188", 18, 0, "-4188", "false"}, {-1, -1, "-12545", 18, 0, "-12545", "false"},
            {-1, -1, "32767", 18, 0, "32767", "false"}, {-1, -1, "-1", 18, 0, "-1", "false"},
            {-1, -1, "-1931", 18, 0, "-1931", "false"}, {-1, -1, "75", 18, 0, "75", "false"},
            {-1, -1, "0", 18, 0, "0", "false"},         {-1, -1, "-440", 18, 0, "-440", "false"},
            {-1, -1, "58", 18, 0, "58", "false"},       {-1, -1, "397", 18, 0, "397", "false"},
            {-1, -1, "51", 18, 0, "51", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "5154", 18, 2, "5154", "false"},     {-1, -1, "2", 18, 2, "2", "false"},
            {-1, -1, "835", 18, 2, "835", "false"},       {-1, -1, "-4", 18, 2, "-4", "false"},
            {-1, -1, "-41", 18, 2, "-41", "false"},       {-1, -1, "32767", 18, 2, "32767", "false"},
            {-1, -1, "-32768", 18, 2, "-32768", "false"}, {-1, -1, "12027", 18, 2, "12027", "false"},
            {-1, -1, "-3", 18, 2, "-3", "false"},         {-1, -1, "-2", 18, 2, "-2", "false"},
            {-1, -1, "58", 18, 2, "58", "false"},         {-1, -1, "-6512", 18, 2, "-6512", "false"},
            {-1, -1, "-2041", 18, 2, "-2041", "false"},   {-1, -1, "29464", 18, 2, "29464", "false"},
            {-1, -1, "-1906", 18, 2, "-1906", "false"},   {-1, -1, "-3415", 18, 2, "-3415", "false"},
            {-1, -1, "31458", 18, 2, "31458", "false"},   {-1, -1, "15421", 18, 2, "15421", "false"},
            {-1, -1, "171", 18, 2, "171", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "0", 18, 9, "0", "false"},         {-1, -1, "7170", 18, 9, "7170", "false"},
            {-1, -1, "-6", 18, 9, "-6", "false"},       {-1, -1, "-1", 18, 9, "-1", "false"},
            {-1, -1, "1029", 18, 9, "1029", "false"},   {-1, -1, "217", 18, 9, "217", "false"},
            {-1, -1, "-27", 18, 9, "-27", "false"},     {-1, -1, "-213", 18, 9, "-213", "false"},
            {-1, -1, "14539", 18, 9, "14539", "false"}, {-1, -1, "-87", 18, 9, "-87", "false"},
            {-1, -1, "32767", 18, 9, "32767", "false"}, {-1, -1, "-7231", 18, 9, "-7231", "false"},
            {-1, -1, "1187", 18, 9, "1187", "false"},   {-1, -1, "1", 18, 9, "1", "false"},
            {-1, -1, "-5444", 18, 9, "-5444", "false"}, {-1, -1, "-2773", 18, 9, "-2773", "false"},
            {-1, -1, "6", 18, 9, "6", "false"},         {-1, -1, "-32768", 18, 9, "-32768", "false"},
            {-1, -1, "-188", 18, 9, "-188", "false"},   {-1, -1, "16734", 18, 9, "16734", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "1613", 18, 18, "", "true"},   {-1, -1, "328", 18, 18, "", "true"},
                                    {-1, -1, "3461", 18, 18, "", "true"},   {-1, -1, "-10", 18, 18, "", "true"},
                                    {-1, -1, "55", 18, 18, "", "true"},     {-1, -1, "-18867", 18, 18, "", "true"},
                                    {-1, -1, "-1147", 18, 18, "", "true"},  {-1, -1, "-21", 18, 18, "", "true"},
                                    {-1, -1, "-16107", 18, 18, "", "true"}, {-1, -1, "-24646", 18, 18, "", "true"},
                                    {-1, -1, "-15366", 18, 18, "", "true"}, {-1, -1, "-2711", 18, 18, "", "true"}};
    test_cast_all_fail<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-1", 18, 18, "-1", "false"}, {-1, -1, "1", 18, 18, "1", "false"},
                                    {-1, -1, "0", 18, 18, "0", "false"},   {-1, -1, "3", 18, 18, "3", "false"},
                                    {-1, -1, "-9", 18, 18, "-9", "false"}, {-1, -1, "6", 18, 18, "6", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "3", 15, 13, "3", "false"},           {-1, -1, "0", 15, 13, "0", "false"},
            {-1, -1, "930", 15, 13, "930", "false"},       {-1, -1, "88", 15, 13, "88", "false"},
            {-1, -1, "224", 15, 13, "224", "false"},       {-1, -1, "15", 15, 13, "15", "false"},
            {-1, -1, "-32768", 15, 13, "-32768", "false"}, {-1, -1, "-122", 15, 13, "-122", "false"},
            {-1, -1, "-46", 15, 13, "-46", "false"},       {-1, -1, "4012", 15, 13, "4012", "false"},
            {-1, -1, "454", 15, 13, "454", "false"},       {-1, -1, "-13496", 15, 13, "-13496", "false"},
            {-1, -1, "-1484", 15, 13, "-1484", "false"},   {-1, -1, "-6513", 15, 13, "-6513", "false"},
            {-1, -1, "-4", 15, 13, "-4", "false"},         {-1, -1, "-16319", 15, 13, "-16319", "false"},
            {-1, -1, "-2595", 15, 13, "-2595", "false"},   {-1, -1, "17830", 15, 13, "17830", "false"},
            {-1, -1, "32767", 15, 13, "32767", "false"},   {-1, -1, "2", 15, 13, "2", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-597", 38, 0, "-597", "false"},     {-1, -1, "2031", 38, 0, "2031", "false"},
            {-1, -1, "-13141", 38, 0, "-13141", "false"}, {-1, -1, "-32768", 38, 0, "-32768", "false"},
            {-1, -1, "125", 38, 0, "125", "false"},       {-1, -1, "-1386", 38, 0, "-1386", "false"},
            {-1, -1, "0", 38, 0, "0", "false"},           {-1, -1, "-8200", 38, 0, "-8200", "false"},
            {-1, -1, "-8099", 38, 0, "-8099", "false"},   {-1, -1, "-7", 38, 0, "-7", "false"},
            {-1, -1, "768", 38, 0, "768", "false"},       {-1, -1, "-18", 38, 0, "-18", "false"},
            {-1, -1, "-10", 38, 0, "-10", "false"},       {-1, -1, "523", 38, 0, "523", "false"},
            {-1, -1, "-145", 38, 0, "-145", "false"},     {-1, -1, "990", 38, 0, "990", "false"},
            {-1, -1, "-5423", 38, 0, "-5423", "false"},   {-1, -1, "-4119", 38, 0, "-4119", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "420", 38, 2, "420", "false"},       {-1, -1, "5038", 38, 2, "5038", "false"},
            {-1, -1, "-3904", 38, 2, "-3904", "false"},   {-1, -1, "-7", 38, 2, "-7", "false"},
            {-1, -1, "-1667", 38, 2, "-1667", "false"},   {-1, -1, "-5", 38, 2, "-5", "false"},
            {-1, -1, "305", 38, 2, "305", "false"},       {-1, -1, "125", 38, 2, "125", "false"},
            {-1, -1, "-55", 38, 2, "-55", "false"},       {-1, -1, "6", 38, 2, "6", "false"},
            {-1, -1, "26", 38, 2, "26", "false"},         {-1, -1, "-16", 38, 2, "-16", "false"},
            {-1, -1, "379", 38, 2, "379", "false"},       {-1, -1, "-502", 38, 2, "-502", "false"},
            {-1, -1, "-10236", 38, 2, "-10236", "false"}, {-1, -1, "7", 38, 2, "7", "false"},
            {-1, -1, "-3851", 38, 2, "-3851", "false"},   {-1, -1, "-32768", 38, 2, "-32768", "false"},
            {-1, -1, "5", 38, 2, "5", "false"},           {-1, -1, "-61", 38, 2, "-61", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-601", 38, 9, "-601", "false"},   {-1, -1, "5615", 38, 9, "5615", "false"},
            {-1, -1, "-416", 38, 9, "-416", "false"},   {-1, -1, "-1199", 38, 9, "-1199", "false"},
            {-1, -1, "-69", 38, 9, "-69", "false"},     {-1, -1, "0", 38, 9, "0", "false"},
            {-1, -1, "-4", 38, 9, "-4", "false"},       {-1, -1, "-2", 38, 9, "-2", "false"},
            {-1, -1, "2518", 38, 9, "2518", "false"},   {-1, -1, "30", 38, 9, "30", "false"},
            {-1, -1, "-68", 38, 9, "-68", "false"},     {-1, -1, "-8", 38, 9, "-8", "false"},
            {-1, -1, "1241", 38, 9, "1241", "false"},   {-1, -1, "271", 38, 9, "271", "false"},
            {-1, -1, "-1041", 38, 9, "-1041", "false"}, {-1, -1, "-3", 38, 9, "-3", "false"},
            {-1, -1, "-244", 38, 9, "-244", "false"},   {-1, -1, "-1724", 38, 9, "-1724", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-1", 38, 13, "-1", "false"},         {-1, -1, "-656", 38, 13, "-656", "false"},
            {-1, -1, "-3755", 38, 13, "-3755", "false"},   {-1, -1, "24", 38, 13, "24", "false"},
            {-1, -1, "-7", 38, 13, "-7", "false"},         {-1, -1, "5088", 38, 13, "5088", "false"},
            {-1, -1, "15869", 38, 13, "15869", "false"},   {-1, -1, "-12192", 38, 13, "-12192", "false"},
            {-1, -1, "-32768", 38, 13, "-32768", "false"}, {-1, -1, "1539", 38, 13, "1539", "false"},
            {-1, -1, "1", 38, 13, "1", "false"},           {-1, -1, "-10732", 38, 13, "-10732", "false"},
            {-1, -1, "7", 38, 13, "7", "false"},           {-1, -1, "-87", 38, 13, "-87", "false"},
            {-1, -1, "-15", 38, 13, "-15", "false"},       {-1, -1, "1262", 38, 13, "1262", "false"},
            {-1, -1, "-28", 38, 13, "-28", "false"},       {-1, -1, "-31028", 38, 13, "-31028", "false"},
            {-1, -1, "15", 38, 13, "15", "false"},         {-1, -1, "-1496", 38, 13, "-1496", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "0", 38, 18, "0", "false"},         {-1, -1, "7913", 38, 18, "7913", "false"},
            {-1, -1, "-3", 38, 18, "-3", "false"},       {-1, -1, "1632", 38, 18, "1632", "false"},
            {-1, -1, "-1824", 38, 18, "-1824", "false"}, {-1, -1, "301", 38, 18, "301", "false"},
            {-1, -1, "-840", 38, 18, "-840", "false"},   {-1, -1, "997", 38, 18, "997", "false"},
            {-1, -1, "32767", 38, 18, "32767", "false"}, {-1, -1, "9", 38, 18, "9", "false"},
            {-1, -1, "1553", 38, 18, "1553", "false"},   {-1, -1, "-878", 38, 18, "-878", "false"},
            {-1, -1, "6714", 38, 18, "6714", "false"},   {-1, -1, "-8628", 38, 18, "-8628", "false"},
            {-1, -1, "19", 38, 18, "19", "false"},       {-1, -1, "188", 38, 18, "188", "false"},
            {-1, -1, "21", 38, 18, "21", "false"},       {-1, -1, "820", 38, 18, "820", "false"},
            {-1, -1, "-111", 38, 18, "-111", "false"},   {-1, -1, "-16371", 38, 18, "-16371", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromSmallIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-770", 35, 30, "-770", "false"},     {-1, -1, "6", 35, 30, "6", "false"},
            {-1, -1, "-32768", 35, 30, "-32768", "false"}, {-1, -1, "-29", 35, 30, "-29", "false"},
            {-1, -1, "-7", 35, 30, "-7", "false"},         {-1, -1, "-1708", 35, 30, "-1708", "false"},
            {-1, -1, "-195", 35, 30, "-195", "false"},     {-1, -1, "-126", 35, 30, "-126", "false"},
            {-1, -1, "1", 35, 30, "1", "false"},           {-1, -1, "-1222", 35, 30, "-1222", "false"},
            {-1, -1, "-3", 35, 30, "-3", "false"},         {-1, -1, "4", 35, 30, "4", "false"},
            {-1, -1, "878", 35, 30, "878", "false"},       {-1, -1, "154", 35, 30, "154", "false"},
            {-1, -1, "12", 35, 30, "12", "false"},         {-1, -1, "-1", 35, 30, "-1", "false"},
            {-1, -1, "1599", 35, 30, "1599", "false"}};
    test_cast_all<TYPE_SMALLINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "67894", 9, 0, "67894", "false"},
                                    {-1, -1, "-4", 9, 0, "-4", "false"},
                                    {-1, -1, "719406", 9, 0, "719406", "false"},
                                    {-1, -1, "0", 9, 0, "0", "false"},
                                    {-1, -1, "-19983", 9, 0, "-19983", "false"},
                                    {-1, -1, "246096184", 9, 0, "246096184", "false"},
                                    {-1, -1, "-60", 9, 0, "-60", "false"},
                                    {-1, -1, "-166103", 9, 0, "-166103", "false"},
                                    {-1, -1, "-520842", 9, 0, "-520842", "false"},
                                    {-1, -1, "126", 9, 0, "126", "false"},
                                    {-1, -1, "136", 9, 0, "136", "false"},
                                    {-1, -1, "16842", 9, 0, "16842", "false"},
                                    {-1, -1, "1594", 9, 0, "1594", "false"},
                                    {-1, -1, "-7", 9, 0, "-7", "false"},
                                    {-1, -1, "677", 9, 0, "677", "false"},
                                    {-1, -1, "-109691", 9, 0, "-109691", "false"},
                                    {-1, -1, "42568993", 9, 0, "42568993", "false"},
                                    {-1, -1, "1467", 9, 0, "1467", "false"},
                                    {-1, -1, "14", 9, 0, "14", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s2Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "48899437", 9, 2, "", "true"},
                                    {-1, -1, "-535591307", 9, 2, "", "true"},
                                    {-1, -1, "-2147483648", 9, 2, "", "true"},
                                    {-1, -1, "-132725535", 9, 2, "", "true"}};
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "113827", 9, 2, "113827", "false"},
                                    {-1, -1, "1494166", 9, 2, "1494166", "false"},
                                    {-1, -1, "6062483", 9, 2, "6062483", "false"},
                                    {-1, -1, "-14173761", 9, 2, "-14173761", "false"},
                                    {-1, -1, "-63", 9, 2, "-63", "false"},
                                    {-1, -1, "-980091", 9, 2, "-980091", "false"},
                                    {-1, -1, "25", 9, 2, "25", "false"},
                                    {-1, -1, "430", 9, 2, "430", "false"},
                                    {-1, -1, "2", 9, 2, "2", "false"},
                                    {-1, -1, "249", 9, 2, "249", "false"},
                                    {-1, -1, "1", 9, 2, "1", "false"},
                                    {-1, -1, "-4142545", 9, 2, "-4142545", "false"},
                                    {-1, -1, "17", 9, 2, "17", "false"},
                                    {-1, -1, "341797", 9, 2, "341797", "false"},
                                    {-1, -1, "218", 9, 2, "218", "false"},
                                    {-1, -1, "-193", 9, 2, "-193", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-64611963", 9, 9, "", "true"},  {-1, -1, "-2147483648", 9, 9, "", "true"},
                                    {-1, -1, "1301", 9, 9, "", "true"},       {-1, -1, "-125637", 9, 9, "", "true"},
                                    {-1, -1, "-266845604", 9, 9, "", "true"}, {-1, -1, "774964674", 9, 9, "", "true"},
                                    {-1, -1, "-6", 9, 9, "", "true"},         {-1, -1, "10808", 9, 9, "", "true"},
                                    {-1, -1, "549", 9, 9, "", "true"},        {-1, -1, "3", 9, 9, "", "true"},
                                    {-1, -1, "378984", 9, 9, "", "true"},     {-1, -1, "-25", 9, 9, "", "true"},
                                    {-1, -1, "5581290", 9, 9, "", "true"},    {-1, -1, "13", 9, 9, "", "true"},
                                    {-1, -1, "-524", 9, 9, "", "true"},       {-1, -1, "-66", 9, 9, "", "true"},
                                    {-1, -1, "41006358", 9, 9, "", "true"},   {-1, -1, "-1235591", 9, 9, "", "true"}};
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "0", 9, 9, "0", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p7s4Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "164210743", 7, 4, "", "true"},  {-1, -1, "8152493", 7, 4, "", "true"},
                                    {-1, -1, "1359132658", 7, 4, "", "true"}, {-1, -1, "-14940301", 7, 4, "", "true"},
                                    {-1, -1, "849164474", 7, 4, "", "true"},  {-1, -1, "20273221", 7, 4, "", "true"},
                                    {-1, -1, "1966608322", 7, 4, "", "true"}, {-1, -1, "47966145", 7, 4, "", "true"}};
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "1", 7, 4, "1", "false"},           {-1, -1, "5", 7, 4, "5", "false"},
            {-1, -1, "903", 7, 4, "903", "false"},       {-1, -1, "130858", 7, 4, "130858", "false"},
            {-1, -1, "52227", 7, 4, "52227", "false"},   {-1, -1, "14", 7, 4, "14", "false"},
            {-1, -1, "-22", 7, 4, "-22", "false"},       {-1, -1, "-8630", 7, 4, "-8630", "false"},
            {-1, -1, "-2", 7, 4, "-2", "false"},         {-1, -1, "0", 7, 4, "0", "false"},
            {-1, -1, "109774", 7, 4, "109774", "false"}, {-1, -1, "-842", 7, 4, "-842", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-139224", 18, 0, "-139224", "false"},
                                    {-1, -1, "-1394346", 18, 0, "-1394346", "false"},
                                    {-1, -1, "4151", 18, 0, "4151", "false"},
                                    {-1, -1, "11", 18, 0, "11", "false"},
                                    {-1, -1, "5593", 18, 0, "5593", "false"},
                                    {-1, -1, "-417100022", 18, 0, "-417100022", "false"},
                                    {-1, -1, "1", 18, 0, "1", "false"},
                                    {-1, -1, "-37", 18, 0, "-37", "false"},
                                    {-1, -1, "1548378", 18, 0, "1548378", "false"},
                                    {-1, -1, "53677749", 18, 0, "53677749", "false"},
                                    {-1, -1, "-2242", 18, 0, "-2242", "false"},
                                    {-1, -1, "1917703666", 18, 0, "1917703666", "false"},
                                    {-1, -1, "-23126", 18, 0, "-23126", "false"},
                                    {-1, -1, "-2147483648", 18, 0, "-2147483648", "false"},
                                    {-1, -1, "48028", 18, 0, "48028", "false"},
                                    {-1, -1, "-1", 18, 0, "-1", "false"},
                                    {-1, -1, "-68", 18, 0, "-68", "false"},
                                    {-1, -1, "-196773174", 18, 0, "-196773174", "false"},
                                    {-1, -1, "4129632", 18, 0, "4129632", "false"},
                                    {-1, -1, "10251", 18, 0, "10251", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "50107509", 18, 2, "50107509", "false"},
                                    {-1, -1, "-1430786", 18, 2, "-1430786", "false"},
                                    {-1, -1, "14657897", 18, 2, "14657897", "false"},
                                    {-1, -1, "-103265", 18, 2, "-103265", "false"},
                                    {-1, -1, "520151511", 18, 2, "520151511", "false"},
                                    {-1, -1, "-1163", 18, 2, "-1163", "false"},
                                    {-1, -1, "-112", 18, 2, "-112", "false"},
                                    {-1, -1, "407", 18, 2, "407", "false"},
                                    {-1, -1, "-9201", 18, 2, "-9201", "false"},
                                    {-1, -1, "-904514848", 18, 2, "-904514848", "false"},
                                    {-1, -1, "269", 18, 2, "269", "false"},
                                    {-1, -1, "-32735", 18, 2, "-32735", "false"},
                                    {-1, -1, "-10", 18, 2, "-10", "false"},
                                    {-1, -1, "-352848182", 18, 2, "-352848182", "false"},
                                    {-1, -1, "-3403", 18, 2, "-3403", "false"},
                                    {-1, -1, "371693656", 18, 2, "371693656", "false"},
                                    {-1, -1, "43261856", 18, 2, "43261856", "false"},
                                    {-1, -1, "-678", 18, 2, "-678", "false"},
                                    {-1, -1, "-1562", 18, 2, "-1562", "false"},
                                    {-1, -1, "2572", 18, 2, "2572", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-69823", 18, 9, "-69823", "false"},
                                    {-1, -1, "-90", 18, 9, "-90", "false"},
                                    {-1, -1, "8260270", 18, 9, "8260270", "false"},
                                    {-1, -1, "-2", 18, 9, "-2", "false"},
                                    {-1, -1, "-2147483648", 18, 9, "-2147483648", "false"},
                                    {-1, -1, "14331969", 18, 9, "14331969", "false"},
                                    {-1, -1, "96", 18, 9, "96", "false"},
                                    {-1, -1, "-5", 18, 9, "-5", "false"},
                                    {-1, -1, "1", 18, 9, "1", "false"},
                                    {-1, -1, "-1588", 18, 9, "-1588", "false"},
                                    {-1, -1, "790", 18, 9, "790", "false"},
                                    {-1, -1, "45", 18, 9, "45", "false"},
                                    {-1, -1, "-223226690", 18, 9, "-223226690", "false"},
                                    {-1, -1, "-22007", 18, 9, "-22007", "false"},
                                    {-1, -1, "-626", 18, 9, "-626", "false"},
                                    {-1, -1, "954981", 18, 9, "954981", "false"},
                                    {-1, -1, "376407525", 18, 9, "376407525", "false"},
                                    {-1, -1, "18913", 18, 9, "18913", "false"},
                                    {-1, -1, "804088155", 18, 9, "804088155", "false"},
                                    {-1, -1, "-1903703481", 18, 9, "-1903703481", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "66277129", 18, 18, "", "true"},  {-1, -1, "18", 18, 18, "", "true"},
            {-1, -1, "-28", 18, 18, "", "true"},       {-1, -1, "-516228", 18, 18, "", "true"},
            {-1, -1, "2415728", 18, 18, "", "true"},   {-1, -1, "-995418", 18, 18, "", "true"},
            {-1, -1, "-246171", 18, 18, "", "true"},   {-1, -1, "11284", 18, 18, "", "true"},
            {-1, -1, "-335026", 18, 18, "", "true"},   {-1, -1, "1153834", 18, 18, "", "true"},
            {-1, -1, "164033948", 18, 18, "", "true"}, {-1, -1, "181", 18, 18, "", "true"},
            {-1, -1, "108555", 18, 18, "", "true"},    {-1, -1, "-9361612", 18, 18, "", "true"},
            {-1, -1, "4615416", 18, 18, "", "true"},   {-1, -1, "-105741", 18, 18, "", "true"},
            {-1, -1, "-127", 18, 18, "", "true"},      {-1, -1, "-534229029", 18, 18, "", "true"}};
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-2", 18, 18, "-2", "false"}, {-1, -1, "-8", 18, 18, "-8", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p15s13Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "2147483647", 15, 13, "", "true"},
                                    {-1, -1, "-8162749", 15, 13, "", "true"},
                                    {-1, -1, "156590116", 15, 13, "", "true"},
                                    {-1, -1, "16507163", 15, 13, "", "true"},
                                    {-1, -1, "-11193414", 15, 13, "", "true"}};
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "14530", 15, 13, "14530", "false"},   {-1, -1, "-1347", 15, 13, "-1347", "false"},
            {-1, -1, "22", 15, 13, "22", "false"},         {-1, -1, "31561", 15, 13, "31561", "false"},
            {-1, -1, "140511", 15, 13, "140511", "false"}, {-1, -1, "7117", 15, 13, "7117", "false"},
            {-1, -1, "48730", 15, 13, "48730", "false"},   {-1, -1, "229116", 15, 13, "229116", "false"},
            {-1, -1, "55985", 15, 13, "55985", "false"},   {-1, -1, "7", 15, 13, "7", "false"},
            {-1, -1, "186", 15, 13, "186", "false"},       {-1, -1, "157536", 15, 13, "157536", "false"},
            {-1, -1, "3695", 15, 13, "3695", "false"},     {-1, -1, "1260", 15, 13, "1260", "false"},
            {-1, -1, "2692", 15, 13, "2692", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "8485310", 38, 0, "8485310", "false"},
                                    {-1, -1, "1680", 38, 0, "1680", "false"},
                                    {-1, -1, "-1", 38, 0, "-1", "false"},
                                    {-1, -1, "-58", 38, 0, "-58", "false"},
                                    {-1, -1, "-601", 38, 0, "-601", "false"},
                                    {-1, -1, "3", 38, 0, "3", "false"},
                                    {-1, -1, "-185966", 38, 0, "-185966", "false"},
                                    {-1, -1, "-122013", 38, 0, "-122013", "false"},
                                    {-1, -1, "2125545", 38, 0, "2125545", "false"},
                                    {-1, -1, "-18540", 38, 0, "-18540", "false"},
                                    {-1, -1, "1519673621", 38, 0, "1519673621", "false"},
                                    {-1, -1, "-10292145", 38, 0, "-10292145", "false"},
                                    {-1, -1, "-2147483648", 38, 0, "-2147483648", "false"},
                                    {-1, -1, "637", 38, 0, "637", "false"},
                                    {-1, -1, "-2033303358", 38, 0, "-2033303358", "false"},
                                    {-1, -1, "-14978452", 38, 0, "-14978452", "false"},
                                    {-1, -1, "303", 38, 0, "303", "false"},
                                    {-1, -1, "445262", 38, 0, "445262", "false"},
                                    {-1, -1, "-18019199", 38, 0, "-18019199", "false"},
                                    {-1, -1, "29", 38, 0, "29", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "248582", 38, 2, "248582", "false"},
                                    {-1, -1, "-11957", 38, 2, "-11957", "false"},
                                    {-1, -1, "-1009", 38, 2, "-1009", "false"},
                                    {-1, -1, "-401", 38, 2, "-401", "false"},
                                    {-1, -1, "-2021694948", 38, 2, "-2021694948", "false"},
                                    {-1, -1, "-2147483648", 38, 2, "-2147483648", "false"},
                                    {-1, -1, "-18122", 38, 2, "-18122", "false"},
                                    {-1, -1, "28348469", 38, 2, "28348469", "false"},
                                    {-1, -1, "856", 38, 2, "856", "false"},
                                    {-1, -1, "1214723330", 38, 2, "1214723330", "false"},
                                    {-1, -1, "-5817574", 38, 2, "-5817574", "false"},
                                    {-1, -1, "11", 38, 2, "11", "false"},
                                    {-1, -1, "-1889083347", 38, 2, "-1889083347", "false"},
                                    {-1, -1, "167", 38, 2, "167", "false"},
                                    {-1, -1, "1949", 38, 2, "1949", "false"},
                                    {-1, -1, "-2", 38, 2, "-2", "false"},
                                    {-1, -1, "3089561", 38, 2, "3089561", "false"},
                                    {-1, -1, "45", 38, 2, "45", "false"},
                                    {-1, -1, "1757506", 38, 2, "1757506", "false"},
                                    {-1, -1, "62464224", 38, 2, "62464224", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-1", 38, 9, "-1", "false"},
                                    {-1, -1, "-409238859", 38, 9, "-409238859", "false"},
                                    {-1, -1, "14", 38, 9, "14", "false"},
                                    {-1, -1, "-2198", 38, 9, "-2198", "false"},
                                    {-1, -1, "231", 38, 9, "231", "false"},
                                    {-1, -1, "961113", 38, 9, "961113", "false"},
                                    {-1, -1, "-46014687", 38, 9, "-46014687", "false"},
                                    {-1, -1, "-3645", 38, 9, "-3645", "false"},
                                    {-1, -1, "-23480", 38, 9, "-23480", "false"},
                                    {-1, -1, "990", 38, 9, "990", "false"},
                                    {-1, -1, "-1915731", 38, 9, "-1915731", "false"},
                                    {-1, -1, "-3", 38, 9, "-3", "false"},
                                    {-1, -1, "-211250", 38, 9, "-211250", "false"},
                                    {-1, -1, "2147483647", 38, 9, "2147483647", "false"},
                                    {-1, -1, "2008769", 38, 9, "2008769", "false"},
                                    {-1, -1, "-5", 38, 9, "-5", "false"},
                                    {-1, -1, "371044", 38, 9, "371044", "false"},
                                    {-1, -1, "1", 38, 9, "1", "false"},
                                    {-1, -1, "0", 38, 9, "0", "false"},
                                    {-1, -1, "-233", 38, 9, "-233", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-737", 38, 13, "-737", "false"},
                                    {-1, -1, "546946459", 38, 13, "546946459", "false"},
                                    {-1, -1, "-1524338446", 38, 13, "-1524338446", "false"},
                                    {-1, -1, "6888296", 38, 13, "6888296", "false"},
                                    {-1, -1, "-5072", 38, 13, "-5072", "false"},
                                    {-1, -1, "355362726", 38, 13, "355362726", "false"},
                                    {-1, -1, "-116", 38, 13, "-116", "false"},
                                    {-1, -1, "430102766", 38, 13, "430102766", "false"},
                                    {-1, -1, "-23294", 38, 13, "-23294", "false"},
                                    {-1, -1, "-59576", 38, 13, "-59576", "false"},
                                    {-1, -1, "963", 38, 13, "963", "false"},
                                    {-1, -1, "664", 38, 13, "664", "false"},
                                    {-1, -1, "278435915", 38, 13, "278435915", "false"},
                                    {-1, -1, "-1896611", 38, 13, "-1896611", "false"},
                                    {-1, -1, "9419013", 38, 13, "9419013", "false"},
                                    {-1, -1, "-321", 38, 13, "-321", "false"},
                                    {-1, -1, "33", 38, 13, "33", "false"},
                                    {-1, -1, "-695", 38, 13, "-695", "false"},
                                    {-1, -1, "2231", 38, 13, "2231", "false"},
                                    {-1, -1, "1451", 38, 13, "1451", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-6", 38, 18, "-6", "false"},
                                    {-1, -1, "5883", 38, 18, "5883", "false"},
                                    {-1, -1, "53", 38, 18, "53", "false"},
                                    {-1, -1, "58", 38, 18, "58", "false"},
                                    {-1, -1, "6", 38, 18, "6", "false"},
                                    {-1, -1, "-7", 38, 18, "-7", "false"},
                                    {-1, -1, "604365", 38, 18, "604365", "false"},
                                    {-1, -1, "-2671939", 38, 18, "-2671939", "false"},
                                    {-1, -1, "4779", 38, 18, "4779", "false"},
                                    {-1, -1, "22", 38, 18, "22", "false"},
                                    {-1, -1, "64337069", 38, 18, "64337069", "false"},
                                    {-1, -1, "-3883", 38, 18, "-3883", "false"},
                                    {-1, -1, "-18946666", 38, 18, "-18946666", "false"},
                                    {-1, -1, "-1813121", 38, 18, "-1813121", "false"},
                                    {-1, -1, "-15874716", 38, 18, "-15874716", "false"},
                                    {-1, -1, "1", 38, 18, "1", "false"},
                                    {-1, -1, "117", 38, 18, "117", "false"},
                                    {-1, -1, "-166527", 38, 18, "-166527", "false"},
                                    {-1, -1, "-223029405", 38, 18, "-223029405", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p35s30Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-748697289", 35, 30, "", "true"},
                                    {-1, -1, "2147483647", 35, 30, "", "true"},
                                    {-1, -1, "-986339122", 35, 30, "", "true"}};
    test_cast_all_fail<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-198107", 35, 30, "-198107", "false"},
                                    {-1, -1, "-10883793", 35, 30, "-10883793", "false"},
                                    {-1, -1, "-145888", 35, 30, "-145888", "false"},
                                    {-1, -1, "-46", 35, 30, "-46", "false"},
                                    {-1, -1, "12272", 35, 30, "12272", "false"},
                                    {-1, -1, "-1660173", 35, 30, "-1660173", "false"},
                                    {-1, -1, "-417", 35, 30, "-417", "false"},
                                    {-1, -1, "-143904", 35, 30, "-143904", "false"},
                                    {-1, -1, "0", 35, 30, "0", "false"},
                                    {-1, -1, "-744086", 35, 30, "-744086", "false"},
                                    {-1, -1, "126", 35, 30, "126", "false"},
                                    {-1, -1, "-43", 35, 30, "-43", "false"},
                                    {-1, -1, "872259", 35, 30, "872259", "false"},
                                    {-1, -1, "-12028863", 35, 30, "-12028863", "false"},
                                    {-1, -1, "136778", 35, 30, "136778", "false"},
                                    {-1, -1, "123", 35, 30, "123", "false"},
                                    {-1, -1, "71722230", 35, 30, "71722230", "false"}};
    test_cast_all<TYPE_INT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s0Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-733984013259202053", 9, 0, "", "true"}, {-1, -1, "-197882106016151", 9, 0, "", "true"},
            {-1, -1, "259947125525", 9, 0, "", "true"},        {-1, -1, "-10408008336359", 9, 0, "", "true"},
            {-1, -1, "-462224191649263049", 9, 0, "", "true"}, {-1, -1, "-86160705728748", 9, 0, "", "true"},
            {-1, -1, "9223372036854775807", 9, 0, "", "true"}, {-1, -1, "-1602341219224696370", 9, 0, "", "true"},
            {-1, -1, "516795814505", 9, 0, "", "true"},        {-1, -1, "-3745071117300741288", 9, 0, "", "true"},
            {-1, -1, "-356113903416", 9, 0, "", "true"},       {-1, -1, "217911718414", 9, 0, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-6616", 9, 0, "-6616", "false"},     {-1, -1, "1811", 9, 0, "1811", "false"},
            {-1, -1, "778203", 9, 0, "778203", "false"},   {-1, -1, "-118224689", 9, 0, "-118224689", "false"},
            {-1, -1, "-186498", 9, 0, "-186498", "false"}, {-1, -1, "38543792", 9, 0, "38543792", "false"},
            {-1, -1, "5237", 9, 0, "5237", "false"},       {-1, -1, "2299133", 9, 0, "2299133", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s2Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-353933135708", 9, 2, "", "true"},        {-1, -1, "364711968", 9, 2, "", "true"},
            {-1, -1, "195216752077868", 9, 2, "", "true"},      {-1, -1, "2675318417", 9, 2, "", "true"},
            {-1, -1, "-9223372036854775808", 9, 2, "", "true"}, {-1, -1, "-17008745101", 9, 2, "", "true"},
            {-1, -1, "-3971175760021593", 9, 2, "", "true"},    {-1, -1, "-29988593281", 9, 2, "", "true"},
            {-1, -1, "654138363354", 9, 2, "", "true"},         {-1, -1, "-1535953759678", 9, 2, "", "true"},
            {-1, -1, "-31598271610", 9, 2, "", "true"},         {-1, -1, "468043761917230821", 9, 2, "", "true"},
            {-1, -1, "1081698385995689270", 9, 2, "", "true"},  {-1, -1, "249015999", 9, 2, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-4881", 9, 2, "-4881", "false"},
                                    {-1, -1, "-4", 9, 2, "-4", "false"},
                                    {-1, -1, "-6778743", 9, 2, "-6778743", "false"},
                                    {-1, -1, "1", 9, 2, "1", "false"},
                                    {-1, -1, "99", 9, 2, "99", "false"},
                                    {-1, -1, "-1", 9, 2, "-1", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "15760413159295", 9, 9, "", "true"},
                                    {-1, -1, "-17478323023603", 9, 9, "", "true"},
                                    {-1, -1, "18482486126", 9, 9, "", "true"},
                                    {-1, -1, "-126", 9, 9, "", "true"},
                                    {-1, -1, "1310", 9, 9, "", "true"},
                                    {-1, -1, "8285979589843605196", 9, 9, "", "true"},
                                    {-1, -1, "1128848609696", 9, 9, "", "true"},
                                    {-1, -1, "1593849", 9, 9, "", "true"},
                                    {-1, -1, "-171801477255444", 9, 9, "", "true"},
                                    {-1, -1, "-6049868", 9, 9, "", "true"},
                                    {-1, -1, "2246492616656033318", 9, 9, "", "true"},
                                    {-1, -1, "-782341", 9, 9, "", "true"},
                                    {-1, -1, "-59421170", 9, 9, "", "true"},
                                    {-1, -1, "9765944689412887", 9, 9, "", "true"},
                                    {-1, -1, "10655", 9, 9, "", "true"},
                                    {-1, -1, "-182459175536", 9, 9, "", "true"},
                                    {-1, -1, "149724559827817141", 9, 9, "", "true"},
                                    {-1, -1, "131621551449", 9, 9, "", "true"},
                                    {-1, -1, "9223372036854775807", 9, 9, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p9s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-1", 9, 9, "-1", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p7s4Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "42870048660502939", 7, 4, "", "true"},    {-1, -1, "-274099738249", 7, 4, "", "true"},
            {-1, -1, "-519157686709083", 7, 4, "", "true"},     {-1, -1, "1217405058902", 7, 4, "", "true"},
            {-1, -1, "-220745945334030", 7, 4, "", "true"},     {-1, -1, "-3324653", 7, 4, "", "true"},
            {-1, -1, "-21225897637", 7, 4, "", "true"},         {-1, -1, "480118942181896228", 7, 4, "", "true"},
            {-1, -1, "-2323630856156642542", 7, 4, "", "true"}, {-1, -1, "-1716112209", 7, 4, "", "true"},
            {-1, -1, "1523907665111", 7, 4, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-12", 7, 4, "-12", "false"},      {-1, -1, "49756", 7, 4, "49756", "false"},
            {-1, -1, "-5", 7, 4, "-5", "false"},        {-1, -1, "3", 7, 4, "3", "false"},
            {-1, -1, "0", 7, 4, "0", "false"},          {-1, -1, "-20656", 7, 4, "-20656", "false"},
            {-1, -1, "-3", 7, 4, "-3", "false"},        {-1, -1, "-68840", 7, 4, "-68840", "false"},
            {-1, -1, "-15980", 7, 4, "-15980", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-1974", 18, 0, "-1974", "false"},
                                    {-1, -1, "-14925026147879784", 18, 0, "-14925026147879784", "false"},
                                    {-1, -1, "172324410650943412", 18, 0, "172324410650943412", "false"},
                                    {-1, -1, "124", 18, 0, "124", "false"},
                                    {-1, -1, "7849305327814618757", 18, 0, "7849305327814618757", "false"},
                                    {-1, -1, "-2830005376", 18, 0, "-2830005376", "false"},
                                    {-1, -1, "11329724685312", 18, 0, "11329724685312", "false"},
                                    {-1, -1, "-8255516120407247398", 18, 0, "-8255516120407247398", "false"},
                                    {-1, -1, "3", 18, 0, "3", "false"},
                                    {-1, -1, "312021644", 18, 0, "312021644", "false"},
                                    {-1, -1, "1128035588", 18, 0, "1128035588", "false"},
                                    {-1, -1, "7216539226874589115", 18, 0, "7216539226874589115", "false"},
                                    {-1, -1, "-15023446147478786", 18, 0, "-15023446147478786", "false"},
                                    {-1, -1, "-277179143896665", 18, 0, "-277179143896665", "false"},
                                    {-1, -1, "2558208717467867544", 18, 0, "2558208717467867544", "false"},
                                    {-1, -1, "945372241366397332", 18, 0, "945372241366397332", "false"},
                                    {-1, -1, "42188722", 18, 0, "42188722", "false"},
                                    {-1, -1, "-14290056576", 18, 0, "-14290056576", "false"},
                                    {-1, -1, "-2589888873976125833", 18, 0, "-2589888873976125833", "false"},
                                    {-1, -1, "39878571", 18, 0, "39878571", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s2Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-412373022386724108", 18, 2, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "179111447946435", 18, 2, "179111447946435", "false"},
                                    {-1, -1, "-14594", 18, 2, "-14594", "false"},
                                    {-1, -1, "8706", 18, 2, "8706", "false"},
                                    {-1, -1, "2914", 18, 2, "2914", "false"},
                                    {-1, -1, "-599476969", 18, 2, "-599476969", "false"},
                                    {-1, -1, "5145045068674", 18, 2, "5145045068674", "false"},
                                    {-1, -1, "-41519", 18, 2, "-41519", "false"},
                                    {-1, -1, "-3644732666", 18, 2, "-3644732666", "false"},
                                    {-1, -1, "-390810026919", 18, 2, "-390810026919", "false"},
                                    {-1, -1, "-499", 18, 2, "-499", "false"},
                                    {-1, -1, "6378332621", 18, 2, "6378332621", "false"},
                                    {-1, -1, "5205090", 18, 2, "5205090", "false"},
                                    {-1, -1, "6855848114", 18, 2, "6855848114", "false"},
                                    {-1, -1, "6971", 18, 2, "6971", "false"},
                                    {-1, -1, "-140847673350", 18, 2, "-140847673350", "false"},
                                    {-1, -1, "-16733509", 18, 2, "-16733509", "false"},
                                    {-1, -1, "1812", 18, 2, "1812", "false"},
                                    {-1, -1, "8644264694295797", 18, 2, "8644264694295797", "false"},
                                    {-1, -1, "23", 18, 2, "23", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s9Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "21382250286", 18, 9, "", "true"},         {-1, -1, "3197082398318004286", 18, 9, "", "true"},
            {-1, -1, "199265761549663941", 18, 9, "", "true"},  {-1, -1, "-22891290831", 18, 9, "", "true"},
            {-1, -1, "5678888406492249504", 18, 9, "", "true"}, {-1, -1, "-71184471751781", 18, 9, "", "true"},
            {-1, -1, "1104332740609237", 18, 9, "", "true"},    {-1, -1, "-18015570166931609", 18, 9, "", "true"},
            {-1, -1, "8770113441290752", 18, 9, "", "true"},    {-1, -1, "-4782003868400919", 18, 9, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-159812317", 18, 9, "-159812317", "false"},
                                    {-1, -1, "841", 18, 9, "841", "false"},
                                    {-1, -1, "229369593", 18, 9, "229369593", "false"},
                                    {-1, -1, "268089", 18, 9, "268089", "false"},
                                    {-1, -1, "5", 18, 9, "5", "false"},
                                    {-1, -1, "17", 18, 9, "17", "false"},
                                    {-1, -1, "3082552", 18, 9, "3082552", "false"},
                                    {-1, -1, "25875", 18, 9, "25875", "false"},
                                    {-1, -1, "-486", 18, 9, "-486", "false"},
                                    {-1, -1, "20625", 18, 9, "20625", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "305337316873", 18, 18, "", "true"},
                                    {-1, -1, "-4388869870456221", 18, 18, "", "true"},
                                    {-1, -1, "41322", 18, 18, "", "true"},
                                    {-1, -1, "-220090", 18, 18, "", "true"},
                                    {-1, -1, "-72002", 18, 18, "", "true"},
                                    {-1, -1, "61395724220436", 18, 18, "", "true"},
                                    {-1, -1, "523999425680907", 18, 18, "", "true"},
                                    {-1, -1, "-2100191147411059", 18, 18, "", "true"},
                                    {-1, -1, "1131753037331940529", 18, 18, "", "true"},
                                    {-1, -1, "-30027", 18, 18, "", "true"},
                                    {-1, -1, "-5639734362148487318", 18, 18, "", "true"},
                                    {-1, -1, "-2801059", 18, 18, "", "true"},
                                    {-1, -1, "-13209231438", 18, 18, "", "true"},
                                    {-1, -1, "179", 18, 18, "", "true"},
                                    {-1, -1, "34176403852285738", 18, 18, "", "true"},
                                    {-1, -1, "-648737385742840962", 18, 18, "", "true"},
                                    {-1, -1, "-25517741492612", 18, 18, "", "true"},
                                    {-1, -1, "177668399212503152", 18, 18, "", "true"},
                                    {-1, -1, "-192772", 18, 18, "", "true"},
                                    {-1, -1, "14467455091", 18, 18, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p15s13Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-3681811", 15, 13, "", "true"},
                                    {-1, -1, "-2264056", 15, 13, "", "true"},
                                    {-1, -1, "-1781764477213701", 15, 13, "", "true"},
                                    {-1, -1, "1345487299267996966", 15, 13, "", "true"},
                                    {-1, -1, "-4604302928906084317", 15, 13, "", "true"},
                                    {-1, -1, "1003444568", 15, 13, "", "true"},
                                    {-1, -1, "-126737963452221", 15, 13, "", "true"},
                                    {-1, -1, "-712326662382", 15, 13, "", "true"},
                                    {-1, -1, "-12377153", 15, 13, "", "true"},
                                    {-1, -1, "13027716284", 15, 13, "", "true"},
                                    {-1, -1, "8095493810579", 15, 13, "", "true"},
                                    {-1, -1, "-18216055137849302", 15, 13, "", "true"},
                                    {-1, -1, "-17225619128", 15, 13, "", "true"},
                                    {-1, -1, "-3470333680617014303", 15, 13, "", "true"},
                                    {-1, -1, "-50146801066", 15, 13, "", "true"},
                                    {-1, -1, "-130939740614", 15, 13, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-1", 15, 13, "-1", "false"},
                                    {-1, -1, "8061", 15, 13, "8061", "false"},
                                    {-1, -1, "0", 15, 13, "0", "false"},
                                    {-1, -1, "1", 15, 13, "1", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "12184370", 38, 0, "12184370", "false"},
                                    {-1, -1, "878103961826", 38, 0, "878103961826", "false"},
                                    {-1, -1, "-340", 38, 0, "-340", "false"},
                                    {-1, -1, "-14", 38, 0, "-14", "false"},
                                    {-1, -1, "-1139577992031", 38, 0, "-1139577992031", "false"},
                                    {-1, -1, "69084311", 38, 0, "69084311", "false"},
                                    {-1, -1, "11019", 38, 0, "11019", "false"},
                                    {-1, -1, "435022802286", 38, 0, "435022802286", "false"},
                                    {-1, -1, "47144337135213892", 38, 0, "47144337135213892", "false"},
                                    {-1, -1, "-36924819961968062", 38, 0, "-36924819961968062", "false"},
                                    {-1, -1, "-16246094", 38, 0, "-16246094", "false"},
                                    {-1, -1, "-225030", 38, 0, "-225030", "false"},
                                    {-1, -1, "3275516654622819034", 38, 0, "3275516654622819034", "false"},
                                    {-1, -1, "612322341302", 38, 0, "612322341302", "false"},
                                    {-1, -1, "-14374", 38, 0, "-14374", "false"},
                                    {-1, -1, "-8067746", 38, 0, "-8067746", "false"},
                                    {-1, -1, "831", 38, 0, "831", "false"},
                                    {-1, -1, "165818982476990", 38, 0, "165818982476990", "false"},
                                    {-1, -1, "356131", 38, 0, "356131", "false"},
                                    {-1, -1, "171244", 38, 0, "171244", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "914166", 38, 2, "914166", "false"},
                                    {-1, -1, "2107037338649474", 38, 2, "2107037338649474", "false"},
                                    {-1, -1, "-232", 38, 2, "-232", "false"},
                                    {-1, -1, "-25002268", 38, 2, "-25002268", "false"},
                                    {-1, -1, "-15273", 38, 2, "-15273", "false"},
                                    {-1, -1, "41190", 38, 2, "41190", "false"},
                                    {-1, -1, "6068624473", 38, 2, "6068624473", "false"},
                                    {-1, -1, "0", 38, 2, "0", "false"},
                                    {-1, -1, "-4063846740494", 38, 2, "-4063846740494", "false"},
                                    {-1, -1, "-35334037394262", 38, 2, "-35334037394262", "false"},
                                    {-1, -1, "-395", 38, 2, "-395", "false"},
                                    {-1, -1, "1333150781711", 38, 2, "1333150781711", "false"},
                                    {-1, -1, "-508", 38, 2, "-508", "false"},
                                    {-1, -1, "-101514001", 38, 2, "-101514001", "false"},
                                    {-1, -1, "-1964380486705261", 38, 2, "-1964380486705261", "false"},
                                    {-1, -1, "-1033999268", 38, 2, "-1033999268", "false"},
                                    {-1, -1, "-9390426277876294", 38, 2, "-9390426277876294", "false"},
                                    {-1, -1, "55948830677156", 38, 2, "55948830677156", "false"},
                                    {-1, -1, "94046904658", 38, 2, "94046904658", "false"},
                                    {-1, -1, "-7058378862893", 38, 2, "-7058378862893", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-179", 38, 9, "-179", "false"},
                                    {-1, -1, "1", 38, 9, "1", "false"},
                                    {-1, -1, "13329206540195", 38, 9, "13329206540195", "false"},
                                    {-1, -1, "-35058645544198221", 38, 9, "-35058645544198221", "false"},
                                    {-1, -1, "-2724491852580098", 38, 9, "-2724491852580098", "false"},
                                    {-1, -1, "679697", 38, 9, "679697", "false"},
                                    {-1, -1, "-3135", 38, 9, "-3135", "false"},
                                    {-1, -1, "3974069415730328", 38, 9, "3974069415730328", "false"},
                                    {-1, -1, "934411227004398", 38, 9, "934411227004398", "false"},
                                    {-1, -1, "-131736365", 38, 9, "-131736365", "false"},
                                    {-1, -1, "-272072615418234", 38, 9, "-272072615418234", "false"},
                                    {-1, -1, "-49022838572", 38, 9, "-49022838572", "false"},
                                    {-1, -1, "101293027", 38, 9, "101293027", "false"},
                                    {-1, -1, "-16413535978644767", 38, 9, "-16413535978644767", "false"},
                                    {-1, -1, "-3437675199", 38, 9, "-3437675199", "false"},
                                    {-1, -1, "402788844280", 38, 9, "402788844280", "false"},
                                    {-1, -1, "234", 38, 9, "234", "false"},
                                    {-1, -1, "-7590889761629", 38, 9, "-7590889761629", "false"},
                                    {-1, -1, "-17313715877", 38, 9, "-17313715877", "false"},
                                    {-1, -1, "-18907769706170", 38, 9, "-18907769706170", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "7681", 38, 13, "7681", "false"},
                                    {-1, -1, "-15837778823339578", 38, 13, "-15837778823339578", "false"},
                                    {-1, -1, "24510", 38, 13, "24510", "false"},
                                    {-1, -1, "-3473312", 38, 13, "-3473312", "false"},
                                    {-1, -1, "1977588737026", 38, 13, "1977588737026", "false"},
                                    {-1, -1, "-4244295622407381448", 38, 13, "-4244295622407381448", "false"},
                                    {-1, -1, "-4381085421596480903", 38, 13, "-4381085421596480903", "false"},
                                    {-1, -1, "-1318528072030205666", 38, 13, "-1318528072030205666", "false"},
                                    {-1, -1, "89523165873", 38, 13, "89523165873", "false"},
                                    {-1, -1, "-161725898697", 38, 13, "-161725898697", "false"},
                                    {-1, -1, "83719240465175406", 38, 13, "83719240465175406", "false"},
                                    {-1, -1, "-567496356049075521", 38, 13, "-567496356049075521", "false"},
                                    {-1, -1, "-8994", 38, 13, "-8994", "false"},
                                    {-1, -1, "-116412430", 38, 13, "-116412430", "false"},
                                    {-1, -1, "-112103684549713392", 38, 13, "-112103684549713392", "false"},
                                    {-1, -1, "-16231031759207355", 38, 13, "-16231031759207355", "false"},
                                    {-1, -1, "-41439754398", 38, 13, "-41439754398", "false"},
                                    {-1, -1, "17059334", 38, 13, "17059334", "false"},
                                    {-1, -1, "-638", 38, 13, "-638", "false"},
                                    {-1, -1, "131244561008814761", 38, 13, "131244561008814761", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-109702231730974488", 38, 18, "-109702231730974488", "false"},
                                    {-1, -1, "683005368", 38, 18, "683005368", "false"},
                                    {-1, -1, "-83560673685185", 38, 18, "-83560673685185", "false"},
                                    {-1, -1, "-50944774462152219", 38, 18, "-50944774462152219", "false"},
                                    {-1, -1, "-230", 38, 18, "-230", "false"},
                                    {-1, -1, "444290419114761", 38, 18, "444290419114761", "false"},
                                    {-1, -1, "15833257119", 38, 18, "15833257119", "false"},
                                    {-1, -1, "12189670539804", 38, 18, "12189670539804", "false"},
                                    {-1, -1, "-447815519625171821", 38, 18, "-447815519625171821", "false"},
                                    {-1, -1, "33402132", 38, 18, "33402132", "false"},
                                    {-1, -1, "-191633224628988667", 38, 18, "-191633224628988667", "false"},
                                    {-1, -1, "388121562", 38, 18, "388121562", "false"},
                                    {-1, -1, "29093869524", 38, 18, "29093869524", "false"},
                                    {-1, -1, "-9223372036854775808", 38, 18, "-9223372036854775808", "false"},
                                    {-1, -1, "-31389", 38, 18, "-31389", "false"},
                                    {-1, -1, "-42529094243860", 38, 18, "-42529094243860", "false"},
                                    {-1, -1, "9878436", 38, 18, "9878436", "false"},
                                    {-1, -1, "32772618", 38, 18, "32772618", "false"},
                                    {-1, -1, "1", 38, 18, "1", "false"},
                                    {-1, -1, "-7902596896671", 38, 18, "-7902596896671", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p35s30Abnormal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-161790336676902630", 35, 30, "", "true"}, {-1, -1, "-11150795461", 35, 30, "", "true"},
            {-1, -1, "-1451840505892", 35, 30, "", "true"},      {-1, -1, "4443038211311", 35, 30, "", "true"},
            {-1, -1, "-4136849619", 35, 30, "", "true"},         {-1, -1, "4304413230558850309", 35, 30, "", "true"},
            {-1, -1, "22774176560", 35, 30, "", "true"},         {-1, -1, "85884874313", 35, 30, "", "true"},
            {-1, -1, "-10279843118", 35, 30, "", "true"},        {-1, -1, "87391488815", 35, 30, "", "true"},
            {-1, -1, "-7527191764827792359", 35, 30, "", "true"}};
    test_cast_all_fail<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromBigIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "18741679", 35, 30, "18741679", "false"},
                                    {-1, -1, "-120", 35, 30, "-120", "false"},
                                    {-1, -1, "-147012", 35, 30, "-147012", "false"},
                                    {-1, -1, "899", 35, 30, "899", "false"},
                                    {-1, -1, "-4", 35, 30, "-4", "false"},
                                    {-1, -1, "-736592", 35, 30, "-736592", "false"},
                                    {-1, -1, "3099", 35, 30, "3099", "false"},
                                    {-1, -1, "-6307792", 35, 30, "-6307792", "false"},
                                    {-1, -1, "-1", 35, 30, "-1", "false"}};
    test_cast_all<TYPE_BIGINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s0Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "1676482344522828658", 9, 0, "", "true"},
                                    {-1, -1, "-18093261755332702448689712513680", 9, 0, "", "true"},
                                    {-1, -1, "-14455250057692942697852419673", 9, 0, "", "true"},
                                    {-1, -1, "-27931438097545444035011350586364698358", 9, 0, "", "true"},
                                    {-1, -1, "-7263018049238490356", 9, 0, "", "true"},
                                    {-1, -1, "-2582066969131091031", 9, 0, "", "true"},
                                    {-1, -1, "-1042378719022", 9, 0, "", "true"},
                                    {-1, -1, "225762724389744161747758898772262862", 9, 0, "", "true"},
                                    {-1, -1, "-22498806704125", 9, 0, "", "true"},
                                    {-1, -1, "20243735492346663973672075962", 9, 0, "", "true"},
                                    {-1, -1, "928594783026213698758235670210449385", 9, 0, "", "true"},
                                    {-1, -1, "-97691195596879", 9, 0, "", "true"},
                                    {-1, -1, "-136056938388", 9, 0, "", "true"},
                                    {-1, -1, "193551204523319338217088121386566", 9, 0, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-9", 9, 0, "-9", "false"},
                                    {-1, -1, "-11", 9, 0, "-11", "false"},
                                    {-1, -1, "-77722434", 9, 0, "-77722434", "false"},
                                    {-1, -1, "591132320", 9, 0, "591132320", "false"},
                                    {-1, -1, "2035825664", 9, 0, "2035825664", "false"},
                                    {-1, -1, "-14569", 9, 0, "-14569", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s2Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-800456383379702", 9, 2, "", "true"},
                                    {-1, -1, "-3883566155320", 9, 2, "", "true"},
                                    {-1, -1, "-40944307189449407411253468461140142450", 9, 2, "", "true"},
                                    {-1, -1, "254325609617253350553", 9, 2, "", "true"},
                                    {-1, -1, "-134070300657325", 9, 2, "", "true"},
                                    {-1, -1, "-491304509891018168405579", 9, 2, "", "true"},
                                    {-1, -1, "576830332982829204485216", 9, 2, "", "true"},
                                    {-1, -1, "-15202765988617075027301328583763761047", 9, 2, "", "true"},
                                    {-1, -1, "38678368426191991637189562944", 9, 2, "", "true"},
                                    {-1, -1, "-8200825181", 9, 2, "", "true"},
                                    {-1, -1, "1528656750598428692967247419718293760", 9, 2, "", "true"},
                                    {-1, -1, "238229426224", 9, 2, "", "true"},
                                    {-1, -1, "-112804016757749035380", 9, 2, "", "true"},
                                    {-1, -1, "525520324001073", 9, 2, "", "true"},
                                    {-1, -1, "-53978834236520440099208463949928153695", 9, 2, "", "true"},
                                    {-1, -1, "-80791383", 9, 2, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-62", 9, 2, "-62", "false"},
                                    {-1, -1, "-24", 9, 2, "-24", "false"},
                                    {-1, -1, "-130635", 9, 2, "-130635", "false"},
                                    {-1, -1, "-1028", 9, 2, "-1028", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p9s9Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "1998973441645971128278670936925748075", 9, 9, "", "true"},
                                    {-1, -1, "-140722583421365178", 9, 9, "", "true"},
                                    {-1, -1, "53679927133740915488306", 9, 9, "", "true"},
                                    {-1, -1, "17662420342", 9, 9, "", "true"},
                                    {-1, -1, "-90545428523170628736889430552917", 9, 9, "", "true"},
                                    {-1, -1, "-52095867081901874531180707", 9, 9, "", "true"},
                                    {-1, -1, "-2044372037631", 9, 9, "", "true"},
                                    {-1, -1, "-5030953018761233249761820149658", 9, 9, "", "true"},
                                    {-1, -1, "-91104837213246326101614476380690", 9, 9, "", "true"},
                                    {-1, -1, "-1865263856888230271955880", 9, 9, "", "true"},
                                    {-1, -1, "-28", 9, 9, "", "true"},
                                    {-1, -1, "-4268084040010132272830069924", 9, 9, "", "true"},
                                    {-1, -1, "7310777828650", 9, 9, "", "true"},
                                    {-1, -1, "-37333", 9, 9, "", "true"},
                                    {-1, -1, "-48900", 9, 9, "", "true"},
                                    {-1, -1, "335", 9, 9, "", "true"},
                                    {-1, -1, "-81714963547030", 9, 9, "", "true"},
                                    {-1, -1, "-70825383443955270641644274823402144736", 9, 9, "", "true"},
                                    {-1, -1, "130107545632715191485226178957007", 9, 9, "", "true"},
                                    {-1, -1, "661376787138245226417157948930091190", 9, 9, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p7s4Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "1186559", 7, 4, "", "true"},
                                    {-1, -1, "84501288035073308588421057783645", 7, 4, "", "true"},
                                    {-1, -1, "2020674441641292248146516", 7, 4, "", "true"},
                                    {-1, -1, "-9000201543451988539", 7, 4, "", "true"},
                                    {-1, -1, "-62890794043001", 7, 4, "", "true"},
                                    {-1, -1, "-4105083193", 7, 4, "", "true"},
                                    {-1, -1, "-249046057", 7, 4, "", "true"},
                                    {-1, -1, "-125889701745934403305391391488063024", 7, 4, "", "true"},
                                    {-1, -1, "70034737129122", 7, 4, "", "true"},
                                    {-1, -1, "15484138052390", 7, 4, "", "true"},
                                    {-1, -1, "-61456688140133595431143941392923737", 7, 4, "", "true"},
                                    {-1, -1, "13491343199034108019814078596871861", 7, 4, "", "true"},
                                    {-1, -1, "-4287247898239355037689776643", 7, 4, "", "true"},
                                    {-1, -1, "1971618252483211646756035894", 7, 4, "", "true"},
                                    {-1, -1, "-103439784672753", 7, 4, "", "true"},
                                    {-1, -1, "23893773106116498617417336", 7, 4, "", "true"},
                                    {-1, -1, "-568336461954919202086966943318289", 7, 4, "", "true"},
                                    {-1, -1, "-6384500205033938613505", 7, 4, "", "true"},
                                    {-1, -1, "-18132171973128469497057645983558592608", 7, 4, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal32p7s4Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "925", 7, 4, "925", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL32>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s0Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-2000351536136662926082078817304105", 18, 0, "", "true"},
                                    {-1, -1, "-7664385617096669113485065081052", 18, 0, "", "true"},
                                    {-1, -1, "-422237204265386893724263358642014", 18, 0, "", "true"},
                                    {-1, -1, "8611102500509080365878660", 18, 0, "", "true"},
                                    {-1, -1, "-22238229136399558052551991088173766", 18, 0, "", "true"},
                                    {-1, -1, "-6677501886515724416690120176778", 18, 0, "", "true"},
                                    {-1, -1, "15795073006098626894381829", 18, 0, "", "true"},
                                    {-1, -1, "161012172277196340820371", 18, 0, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s0Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "0", 18, 0, "0", "false"},
                                    {-1, -1, "68569751589646", 18, 0, "68569751589646", "false"},
                                    {-1, -1, "-72230133836856089", 18, 0, "-72230133836856089", "false"},
                                    {-1, -1, "-54393308199132731", 18, 0, "-54393308199132731", "false"},
                                    {-1, -1, "945146", 18, 0, "945146", "false"},
                                    {-1, -1, "8114474083", 18, 0, "8114474083", "false"},
                                    {-1, -1, "304044382111029", 18, 0, "304044382111029", "false"},
                                    {-1, -1, "6991240578188397544", 18, 0, "6991240578188397544", "false"},
                                    {-1, -1, "501535", 18, 0, "501535", "false"},
                                    {-1, -1, "2193276", 18, 0, "2193276", "false"},
                                    {-1, -1, "479229", 18, 0, "479229", "false"},
                                    {-1, -1, "483131176200", 18, 0, "483131176200", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s2Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "473126982069097391079495498614244102", 18, 2, "", "true"},
                                    {-1, -1, "6135703342614741443794676", 18, 2, "", "true"},
                                    {-1, -1, "-6523182115153476321514884129505016316", 18, 2, "", "true"},
                                    {-1, -1, "-366986813498400921064633304833578030", 18, 2, "", "true"},
                                    {-1, -1, "-33007337729466645096380", 18, 2, "", "true"},
                                    {-1, -1, "-222867309396595365501", 18, 2, "", "true"},
                                    {-1, -1, "-10411037787589832972082502769443", 18, 2, "", "true"},
                                    {-1, -1, "-45189328094000607690213018234078", 18, 2, "", "true"},
                                    {-1, -1, "-4921057260909991187250", 18, 2, "", "true"},
                                    {-1, -1, "-838028860297487232", 18, 2, "", "true"},
                                    {-1, -1, "-27575849748989061846", 18, 2, "", "true"},
                                    {-1, -1, "-1245549527302707719", 18, 2, "", "true"},
                                    {-1, -1, "503236201311679543", 18, 2, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s2Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "1541897", 18, 2, "1541897", "false"},
                                    {-1, -1, "954103930", 18, 2, "954103930", "false"},
                                    {-1, -1, "121203", 18, 2, "121203", "false"},
                                    {-1, -1, "-7582", 18, 2, "-7582", "false"},
                                    {-1, -1, "-719342", 18, 2, "-719342", "false"},
                                    {-1, -1, "-1172912847", 18, 2, "-1172912847", "false"},
                                    {-1, -1, "100653239307", 18, 2, "100653239307", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s9Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "998841958412415680107326840664547618", 18, 9, "", "true"},
                                    {-1, -1, "836135994104222731685568", 18, 9, "", "true"},
                                    {-1, -1, "7061594249990290", 18, 9, "", "true"},
                                    {-1, -1, "1173616550967457756439437274", 18, 9, "", "true"},
                                    {-1, -1, "-127283203089", 18, 9, "", "true"},
                                    {-1, -1, "13788763169371546527132", 18, 9, "", "true"},
                                    {-1, -1, "-105624770733249786", 18, 9, "", "true"},
                                    {-1, -1, "-13203522812181857447958297943897284347", 18, 9, "", "true"},
                                    {-1, -1, "3789418108896731035949616811", 18, 9, "", "true"},
                                    {-1, -1, "-1402490618506", 18, 9, "", "true"},
                                    {-1, -1, "2806854793120881721638868285819", 18, 9, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s9Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "263763515", 18, 9, "263763515", "false"},
                                    {-1, -1, "1317", 18, 9, "1317", "false"},
                                    {-1, -1, "-5067982", 18, 9, "-5067982", "false"},
                                    {-1, -1, "-5018", 18, 9, "-5018", "false"},
                                    {-1, -1, "-179", 18, 9, "-179", "false"},
                                    {-1, -1, "3253", 18, 9, "3253", "false"},
                                    {-1, -1, "3160553", 18, 9, "3160553", "false"},
                                    {-1, -1, "123728", 18, 9, "123728", "false"},
                                    {-1, -1, "-480", 18, 9, "-480", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s18Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-38518761103363359583480056", 18, 18, "", "true"},
                                    {-1, -1, "2408304", 18, 18, "", "true"},
                                    {-1, -1, "-32599406424612823064450529900349038", 18, 18, "", "true"},
                                    {-1, -1, "-2930827326601731", 18, 18, "", "true"},
                                    {-1, -1, "464832979573278845096169636", 18, 18, "", "true"},
                                    {-1, -1, "135704872465789559", 18, 18, "", "true"},
                                    {-1, -1, "-15140573", 18, 18, "", "true"},
                                    {-1, -1, "2983591254947906096674933196853135", 18, 18, "", "true"},
                                    {-1, -1, "-106815515", 18, 18, "", "true"},
                                    {-1, -1, "847098", 18, 18, "", "true"},
                                    {-1, -1, "37994306283106692899", 18, 18, "", "true"},
                                    {-1, -1, "-214173371974478527619247", 18, 18, "", "true"},
                                    {-1, -1, "12680015907250", 18, 18, "", "true"},
                                    {-1, -1, "5473330337793080936496409933", 18, 18, "", "true"},
                                    {-1, -1, "-1211395564540258", 18, 18, "", "true"},
                                    {-1, -1, "-10258068579158527088230", 18, 18, "", "true"},
                                    {-1, -1, "485801798781388881992168458", 18, 18, "", "true"},
                                    {-1, -1, "14078117124386391553524335634120", 18, 18, "", "true"},
                                    {-1, -1, "1366", 18, 18, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p18s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "1", 18, 18, "1", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p15s13Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-1187309854732957625686829410420280", 15, 13, "", "true"},
                                    {-1, -1, "-46781607720146983764539139", 15, 13, "", "true"},
                                    {-1, -1, "52803731496543089233860806703993248791", 15, 13, "", "true"},
                                    {-1, -1, "6280496730769958823351927947788", 15, 13, "", "true"},
                                    {-1, -1, "-19950374555593412941160838939648", 15, 13, "", "true"},
                                    {-1, -1, "1388131348575083375923125141", 15, 13, "", "true"},
                                    {-1, -1, "76497013808042850583037574759", 15, 13, "", "true"},
                                    {-1, -1, "-17717733571985059465007776276", 15, 13, "", "true"},
                                    {-1, -1, "-5970031065124", 15, 13, "", "true"},
                                    {-1, -1, "-106751078", 15, 13, "", "true"},
                                    {-1, -1, "-1198086383854826919125148869272880", 15, 13, "", "true"},
                                    {-1, -1, "147079341622694271980419088", 15, 13, "", "true"},
                                    {-1, -1, "149933413606218524394408134", 15, 13, "", "true"},
                                    {-1, -1, "-21593108427623756893751670471977796", 15, 13, "", "true"},
                                    {-1, -1, "-27746375569160308900", 15, 13, "", "true"},
                                    {-1, -1, "3893212827135408", 15, 13, "", "true"},
                                    {-1, -1, "76852965852263125772630453", 15, 13, "", "true"},
                                    {-1, -1, "118082305862", 15, 13, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal64p15s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "-868634", 15, 13, "-868634", "false"},
                                    {-1, -1, "166722", 15, 13, "166722", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL64>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s0Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "69858553529707272281913149793561", 38, 0, "69858553529707272281913149793561", "false"},
            {-1, -1, "-1179805859395930483380212", 38, 0, "-1179805859395930483380212", "false"},
            {-1, -1, "2284183992175260930642457", 38, 0, "2284183992175260930642457", "false"},
            {-1, -1, "6311866999769588329531794229", 38, 0, "6311866999769588329531794229", "false"},
            {-1, -1, "-3592783974973565163443817698106578910", 38, 0, "-3592783974973565163443817698106578910",
             "false"},
            {-1, -1, "-10056408051233328263734664723118", 38, 0, "-10056408051233328263734664723118", "false"},
            {-1, -1, "-208408494567", 38, 0, "-208408494567", "false"},
            {-1, -1, "-3795663700", 38, 0, "-3795663700", "false"},
            {-1, -1, "-12549846670108987879596989353907039", 38, 0, "-12549846670108987879596989353907039", "false"},
            {-1, -1, "-275305952891146326675883424", 38, 0, "-275305952891146326675883424", "false"},
            {-1, -1, "1640005337522", 38, 0, "1640005337522", "false"},
            {-1, -1, "-22739734090824", 38, 0, "-22739734090824", "false"},
            {-1, -1, "281693382546060", 38, 0, "281693382546060", "false"},
            {-1, -1, "3007917565928411", 38, 0, "3007917565928411", "false"},
            {-1, -1, "-217467857650244216285373361", 38, 0, "-217467857650244216285373361", "false"},
            {-1, -1, "131003", 38, 0, "131003", "false"},
            {-1, -1, "-280481403382125147872490253", 38, 0, "-280481403382125147872490253", "false"},
            {-1, -1, "-1433742084941", 38, 0, "-1433742084941", "false"},
            {-1, -1, "-2852940771", 38, 0, "-2852940771", "false"},
            {-1, -1, "-411830757620010534536912545767", 38, 0, "-411830757620010534536912545767", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s2Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "-44949738320684393446381079403132", 38, 2, "-44949738320684393446381079403132", "false"},
            {-1, -1, "-635695", 38, 2, "-635695", "false"},
            {-1, -1, "7902586784815788183711", 38, 2, "7902586784815788183711", "false"},
            {-1, -1, "-799227007140241171515163713", 38, 2, "-799227007140241171515163713", "false"},
            {-1, -1, "-67906", 38, 2, "-67906", "false"},
            {-1, -1, "-735811", 38, 2, "-735811", "false"},
            {-1, -1, "-3484755542661771373222918", 38, 2, "-3484755542661771373222918", "false"},
            {-1, -1, "-44972485", 38, 2, "-44972485", "false"},
            {-1, -1, "-57919815200062923", 38, 2, "-57919815200062923", "false"},
            {-1, -1, "-529832000872681657727", 38, 2, "-529832000872681657727", "false"},
            {-1, -1, "0", 38, 2, "0", "false"},
            {-1, -1, "100651904606093062485", 38, 2, "100651904606093062485", "false"},
            {-1, -1, "21820657347", 38, 2, "21820657347", "false"},
            {-1, -1, "-871590582251775287", 38, 2, "-871590582251775287", "false"},
            {-1, -1, "-446734833816050164", 38, 2, "-446734833816050164", "false"},
            {-1, -1, "16620372132040573360696129227", 38, 2, "16620372132040573360696129227", "false"},
            {-1, -1, "-105351571835461", 38, 2, "-105351571835461", "false"},
            {-1, -1, "476", 38, 2, "476", "false"},
            {-1, -1, "-9194311993090500720", 38, 2, "-9194311993090500720", "false"},
            {-1, -1, "-6416289964", 38, 2, "-6416289964", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s9Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "8687739038764787043432191044033164430", 38, 9, "", "true"},
                                    {-1, -1, "170141183460469231731687303715884105727", 38, 9, "", "true"},
                                    {-1, -1, "-2889410470202484480434731888456592258", 38, 9, "", "true"},
                                    {-1, -1, "-489439816566472801991842780194724", 38, 9, "", "true"},
                                    {-1, -1, "-4788595682559849290949209487144", 38, 9, "", "true"},
                                    {-1, -1, "-188739560553637417205545591395", 38, 9, "", "true"},
                                    {-1, -1, "-260581566657755975597344224111", 38, 9, "", "true"},
                                    {-1, -1, "-247047313389116644726645436646", 38, 9, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s9Normal) {
    CastTestCaseArray test_cases = {
            {-1, -1, "35170199332856106292", 38, 9, "35170199332856106292", "false"},
            {-1, -1, "632624710680430242389045", 38, 9, "632624710680430242389045", "false"},
            {-1, -1, "187650181", 38, 9, "187650181", "false"},
            {-1, -1, "-276393546423", 38, 9, "-276393546423", "false"},
            {-1, -1, "-255823105130481504734607385", 38, 9, "-255823105130481504734607385", "false"},
            {-1, -1, "5903090217417102882055688784", 38, 9, "5903090217417102882055688784", "false"},
            {-1, -1, "-7184680", 38, 9, "-7184680", "false"},
            {-1, -1, "-808146102683417", 38, 9, "-808146102683417", "false"},
            {-1, -1, "26752", 38, 9, "26752", "false"},
            {-1, -1, "32794", 38, 9, "32794", "false"},
            {-1, -1, "60536924055682", 38, 9, "60536924055682", "false"},
            {-1, -1, "-106699900", 38, 9, "-106699900", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s13Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "33094138259661154325431291530", 38, 13, "", "true"},
                                    {-1, -1, "2318161215485389246542634678", 38, 13, "", "true"},
                                    {-1, -1, "36109148148733805430655874797359", 38, 13, "", "true"},
                                    {-1, -1, "81177877972554250970903279475700", 38, 13, "", "true"},
                                    {-1, -1, "-493492240451920124932499981", 38, 13, "", "true"},
                                    {-1, -1, "-527756184261379113343583134216650371", 38, 13, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s13Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "4400188408329052789", 38, 13, "4400188408329052789", "false"},
                                    {-1, -1, "-18080830777", 38, 13, "-18080830777", "false"},
                                    {-1, -1, "652434855049", 38, 13, "652434855049", "false"},
                                    {-1, -1, "906394244", 38, 13, "906394244", "false"},
                                    {-1, -1, "252090410890457936", 38, 13, "252090410890457936", "false"},
                                    {-1, -1, "-5472418296196", 38, 13, "-5472418296196", "false"},
                                    {-1, -1, "1576538815574497158917207", 38, 13, "1576538815574497158917207", "false"},
                                    {-1, -1, "-224", 38, 13, "-224", "false"},
                                    {-1, -1, "382137160735", 38, 13, "382137160735", "false"},
                                    {-1, -1, "127", 38, 13, "127", "false"},
                                    {-1, -1, "-5432122119103", 38, 13, "-5432122119103", "false"},
                                    {-1, -1, "-22983274301466319054", 38, 13, "-22983274301466319054", "false"},
                                    {-1, -1, "591837589727708", 38, 13, "591837589727708", "false"},
                                    {-1, -1, "1739001", 38, 13, "1739001", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s18Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "2422502809279453616961311332383206", 38, 18, "", "true"},
                                    {-1, -1, "250811768984805774405490069381", 38, 18, "", "true"},
                                    {-1, -1, "-110001182842337862243808755", 38, 18, "", "true"},
                                    {-1, -1, "-7798588819838971201590936683418193", 38, 18, "", "true"},
                                    {-1, -1, "4120867886031060104916532244", 38, 18, "", "true"},
                                    {-1, -1, "-180126121861933240329009894785", 38, 18, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p38s18Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "1", 38, 18, "1", "false"},
                                    {-1, -1, "32665268", 38, 18, "32665268", "false"},
                                    {-1, -1, "-4150723193691", 38, 18, "-4150723193691", "false"},
                                    {-1, -1, "-13872668481", 38, 18, "-13872668481", "false"},
                                    {-1, -1, "-99916514", 38, 18, "-99916514", "false"},
                                    {-1, -1, "118529641967", 38, 18, "118529641967", "false"},
                                    {-1, -1, "1061", 38, 18, "1061", "false"},
                                    {-1, -1, "161014653", 38, 18, "161014653", "false"},
                                    {-1, -1, "1949522", 38, 18, "1949522", "false"},
                                    {-1, -1, "-3", 38, 18, "-3", "false"},
                                    {-1, -1, "50468850", 38, 18, "50468850", "false"},
                                    {-1, -1, "-22962379514337937361", 38, 18, "-22962379514337937361", "false"},
                                    {-1, -1, "-112210796894555241", 38, 18, "-112210796894555241", "false"},
                                    {-1, -1, "-3078056", 38, 18, "-3078056", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p35s30Abnormal) {
    CastTestCaseArray test_cases = {{-1, -1, "-8952985878682445", 35, 30, "", "true"},
                                    {-1, -1, "-300056902586052242473989008215153297", 35, 30, "", "true"},
                                    {-1, -1, "7241499620753975041226772156823579", 35, 30, "", "true"},
                                    {-1, -1, "-37134896071808347443", 35, 30, "", "true"},
                                    {-1, -1, "-3556961353663961506369631403914012", 35, 30, "", "true"},
                                    {-1, -1, "1862055688121682", 35, 30, "", "true"},
                                    {-1, -1, "2739024111698593285168117902314391403", 35, 30, "", "true"},
                                    {-1, -1, "-604955383321326116786994400", 35, 30, "", "true"},
                                    {-1, -1, "3916730383358139793515144560922403", 35, 30, "", "true"},
                                    {-1, -1, "-805109900300749", 35, 30, "", "true"},
                                    {-1, -1, "1070140851421894503346937743695", 35, 30, "", "true"},
                                    {-1, -1, "60105814621251864402310401430108961028", 35, 30, "", "true"},
                                    {-1, -1, "-170141183460469231731687303715884105728", 35, 30, "", "true"},
                                    {-1, -1, "4377495216897350", 35, 30, "", "true"},
                                    {-1, -1, "1661959969314500293", 35, 30, "", "true"}};
    test_cast_all_fail<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

PARALLEL_TEST(VectorizedDecimalCastExprIntegerTest, testCastFromLargeIntToDecimal128p35s30Normal) {
    CastTestCaseArray test_cases = {{-1, -1, "19102097", 35, 30, "19102097", "false"},
                                    {-1, -1, "-1427", 35, 30, "-1427", "false"},
                                    {-1, -1, "-7755", 35, 30, "-7755", "false"},
                                    {-1, -1, "1027", 35, 30, "1027", "false"},
                                    {-1, -1, "125917", 35, 30, "125917", "false"}};
    test_cast_all<TYPE_LARGEINT, TYPE_DECIMAL128>(test_cases);
}

} // namespace starrocks
