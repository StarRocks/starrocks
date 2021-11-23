#!/usr/bin/env python
# encoding: utf-8

# This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

# The format is:
#   <function id> <name>, <return_type>, [<args>], <backend fn>,
# With an optional
#   <prepare fn>, <close fn>
#
# example:
#   [1, "add", "TINYINT", ["TINYINT", "TINYINT"], "Math::add", "Math::add_prepare", "Math::add_close"]
#
# the id rule: {module function group}|0|{function group}|{sub-function/alias-function}
#
# example round functions: 1               013             0       = 10130
#                          ^                ^              ^
#                   {math function} {function group} {sub-function}
vectorized_functions = [
    # 10xxx: math functions
    [10010, "pi", "DOUBLE", [], "MathFunctions::pi"],
    [10020, "e", "DOUBLE", [], "MathFunctions::e"],
    [10030, "sign", "FLOAT", ["DOUBLE"], "MathFunctions::sign"],

    [10040, "abs", "DOUBLE", ["DOUBLE"], "MathFunctions::abs_double"],
    [10041, "abs", "FLOAT", ["FLOAT"], "MathFunctions::abs_float"],
    [10042, "abs", "LARGEINT", ["LARGEINT"], "MathFunctions::abs_largeint"],
    [10043, "abs", "LARGEINT", ["BIGINT"], "MathFunctions::abs_bigint"],
    [10044, "abs", "BIGINT", ["INT"], "MathFunctions::abs_int"],
    [10045, "abs", "INT", ["SMALLINT"], "MathFunctions::abs_smallint"],
    [10046, "abs", "SMALLINT", ["TINYINT"], "MathFunctions::abs_tinyint"],
    [10047, "abs", "DECIMALV2", ["DECIMALV2"], "MathFunctions::abs_decimalv2val"],
    [100470, "abs", "DECIMAL32", ["DECIMAL32"], "MathFunctions::abs_decimal32"],
    [100471, "abs", "DECIMAL64", ["DECIMAL64"], "MathFunctions::abs_decimal64"],
    [100472, "abs", "DECIMAL128", ["DECIMAL128"], "MathFunctions::abs_decimal128"],

    [10050, "sin", "DOUBLE", ["DOUBLE"], "MathFunctions::sin"],
    [10060, "asin", "DOUBLE", ["DOUBLE"], "MathFunctions::asin"],
    [10070, "cos", "DOUBLE", ["DOUBLE"], "MathFunctions::cos"],
    [10080, "acos", "DOUBLE", ["DOUBLE"], "MathFunctions::acos"],
    [10090, "tan", "DOUBLE", ["DOUBLE"], "MathFunctions::tan"],
    [10100, "atan", "DOUBLE", ["DOUBLE"], "MathFunctions::atan"],

    [10110, "ceil", "BIGINT", ["DOUBLE"], "MathFunctions::ceil"],
    [10111, "ceiling", "BIGINT", ["DOUBLE"], "MathFunctions::ceil"],
    [10112, "dceil", "BIGINT", ["DOUBLE"], "MathFunctions::ceil"],

    [10120, "floor", "BIGINT", ["DOUBLE"], "MathFunctions::floor"],
    [10121, "dfloor", "BIGINT", ["DOUBLE"], "MathFunctions::floor"],

    [10130, "round", "BIGINT", ["DOUBLE"], "MathFunctions::round"],
    [10131, "dround", "BIGINT", ["DOUBLE"], "MathFunctions::round"],
    [10132, "round", "DOUBLE", ["DOUBLE", "INT"], "MathFunctions::round_up_to"],
    [10133, "dround", "DOUBLE", ["DOUBLE", "INT"], "MathFunctions::round_up_to"],
    [10134, "truncate", "DOUBLE", ["DOUBLE", "INT"], "MathFunctions::truncate"],

    [10140, "ln", "DOUBLE", ["DOUBLE"], "MathFunctions::ln"],
    [10141, "dlog1", "DOUBLE", ["DOUBLE"], "MathFunctions::ln"],
    [10142, "log", "DOUBLE", ["DOUBLE"], "MathFunctions::ln"],

    [10150, "log", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::log"],
    [10160, "log2", "DOUBLE", ["DOUBLE"], "MathFunctions::log2"],

    [10170, "log10", "DOUBLE", ["DOUBLE"], "MathFunctions::log10"],
    [10171, "dlog10", "DOUBLE", ["DOUBLE"], "MathFunctions::log10"],

    [10180, "exp", "DOUBLE", ["DOUBLE"], "MathFunctions::exp"],
    [10181, "dexp", "DOUBLE", ["DOUBLE"], "MathFunctions::exp"],

    [10190, "radians", "DOUBLE", ["DOUBLE"], "MathFunctions::radians"],
    [10200, "degrees", "DOUBLE", ["DOUBLE"], "MathFunctions::degrees"],

    [10210, "sqrt", "DOUBLE", ["DOUBLE"], "MathFunctions::sqrt"],
    [10211, "dsqrt", "DOUBLE", ["DOUBLE"], "MathFunctions::sqrt"],

    [10220, "pow", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::pow"],
    [10221, "power", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::pow"],
    [10222, "dpow", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::pow"],
    [10223, "fpow", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::pow"],

    [10224, "atan2", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::atan2"],
    [10225, "cot", "DOUBLE", ["DOUBLE"], "MathFunctions::cot"],

    [10230, "pmod", "BIGINT", ["BIGINT", "BIGINT"], "MathFunctions::pmod<TYPE_BIGINT>"],
    [10231, "pmod", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::pmod<TYPE_DOUBLE>"],

    [10240, "fmod", "FLOAT", ["FLOAT", "FLOAT"], "MathFunctions::fmod<TYPE_FLOAT>"],
    [10241, "fmod", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::fmod<TYPE_DOUBLE>"],

    [10250, "mod", "TINYINT", ["TINYINT", "TINYINT"], "MathFunctions::mod<TYPE_TINYINT>"],
    [10251, "mod", "SMALLINT", ["SMALLINT", "SMALLINT"], "MathFunctions::mod<TYPE_SMALLINT>"],
    [10252, "mod", "INT", ["INT", "INT"], "MathFunctions::mod<TYPE_INT>"],
    [10253, "mod", "BIGINT", ["BIGINT", "BIGINT"], "MathFunctions::mod<TYPE_BIGINT>"],
    [10254, "mod", "LARGEINT", ["LARGEINT", "LARGEINT"], "MathFunctions::mod<TYPE_LARGEINT>"],
    [10255, "mod", "FLOAT", ["FLOAT", "FLOAT"], "MathFunctions::fmod<TYPE_FLOAT>"],
    [10256, "mod", "DOUBLE", ["DOUBLE", "DOUBLE"], "MathFunctions::fmod<TYPE_DOUBLE>"],
    [10257, "mod", "DECIMALV2", ["DECIMALV2", "DECIMALV2"], "MathFunctions::mod<TYPE_DECIMALV2>"],
    [102570, "mod", "DECIMAL32", ["DECIMAL32", "DECIMAL32"], "MathFunctions::mod<TYPE_DECIMAL32>"],
    [102571, "mod", "DECIMAL64", ["DECIMAL64", "DECIMAL64"], "MathFunctions::mod<TYPE_DECIMAL64>"],
    [102572, "mod", "DECIMAL128", ["DECIMAL128", "DECIMAL128"], "MathFunctions::mod<TYPE_DECIMAL128>"],

    [10260, "positive", "DOUBLE", ["DOUBLE"], "MathFunctions::positive<TYPE_DOUBLE>"],
    [10261, "positive", "BIGINT", ["BIGINT"], "MathFunctions::positive<TYPE_BIGINT>"],
    [10262, "positive", "DECIMALV2", ["DECIMALV2"], "MathFunctions::positive<TYPE_DECIMALV2>"],
    [102620, "positive", "DECIMAL32", ["DECIMAL32"], "MathFunctions::positive<TYPE_DECIMAL32>"],
    [102621, "positive", "DECIMAL64", ["DECIMAL64"], "MathFunctions::positive<TYPE_DECIMAL64>"],
    [102622, "positive", "DECIMAL128", ["DECIMAL128"], "MathFunctions::positive<TYPE_DECIMAL128>"],

    [10270, "negative", "DOUBLE", ["DOUBLE"], "MathFunctions::negative<TYPE_DOUBLE>"],
    [10271, "negative", "BIGINT", ["BIGINT"], "MathFunctions::negative<TYPE_BIGINT>"],
    [10272, "negative", "DECIMALV2", ["DECIMALV2"], "MathFunctions::negative<TYPE_DECIMALV2>"],
    [102720, "negative", "DECIMAL32", ["DECIMAL32"], "MathFunctions::negative<TYPE_DECIMAL32>"],
    [102721, "negative", "DECIMAL64", ["DECIMAL64"], "MathFunctions::negative<TYPE_DECIMAL64>"],
    [102722, "negative", "DECIMAL128", ["DECIMAL128"], "MathFunctions::negative<TYPE_DECIMAL128>"],

    [10280, "least", "TINYINT", ["TINYINT", "..."], "MathFunctions::least<TYPE_TINYINT>"],
    [10281, "least", "SMALLINT", ["SMALLINT", "..."], "MathFunctions::least<TYPE_SMALLINT>"],
    [10282, "least", "INT", ["INT", "..."], "MathFunctions::least<TYPE_INT>"],
    [10283, "least", "BIGINT", ["BIGINT", "..."], "MathFunctions::least<TYPE_BIGINT>"],
    [10284, "least", "LARGEINT", ["LARGEINT", "..."], "MathFunctions::least<TYPE_LARGEINT>"],
    [10285, "least", "FLOAT", ["FLOAT", "..."], "MathFunctions::least<TYPE_FLOAT>"],
    [10286, "least", "DOUBLE", ["DOUBLE", "..."], "MathFunctions::least<TYPE_DOUBLE>"],
    [10287, "least", "DECIMALV2", ["DECIMALV2", "..."], "MathFunctions::least<TYPE_DECIMALV2>"],
    [102870, "least", "DECIMAL32", ["DECIMAL32", "..."], "MathFunctions::least<TYPE_DECIMAL32>"],
    [102871, "least", "DECIMAL64", ["DECIMAL64", "..."], "MathFunctions::least<TYPE_DECIMAL64>"],
    [102872, "least", "DECIMAL128", ["DECIMAL128", "..."], "MathFunctions::least<TYPE_DECIMAL128>"],
    [10288, "least", "DATETIME", ["DATETIME", "..."], "MathFunctions::least<TYPE_DATETIME>"],
    [10289, "least", "VARCHAR", ["VARCHAR", "..."], "MathFunctions::least<TYPE_VARCHAR>"],

    [10290, "greatest", "TINYINT", ["TINYINT", "..."], "MathFunctions::greatest<TYPE_TINYINT>"],
    [10291, "greatest", "SMALLINT", ["SMALLINT", "..."], "MathFunctions::greatest<TYPE_SMALLINT>"],
    [10292, "greatest", "INT", ["INT", "..."], "MathFunctions::greatest<TYPE_INT>"],
    [10293, "greatest", "BIGINT", ["BIGINT", "..."], "MathFunctions::greatest<TYPE_BIGINT>"],
    [10294, "greatest", "LARGEINT", ["LARGEINT", "..."], "MathFunctions::greatest<TYPE_LARGEINT>"],
    [10295, "greatest", "FLOAT", ["FLOAT", "..."], "MathFunctions::greatest<TYPE_FLOAT>"],
    [10296, "greatest", "DOUBLE", ["DOUBLE", "..."], "MathFunctions::greatest<TYPE_DOUBLE>"],
    [10297, "greatest", "DECIMALV2", ["DECIMALV2", "..."], "MathFunctions::greatest<TYPE_DECIMALV2>"],
    [102970, "greatest", "DECIMAL32", ["DECIMAL32", "..."], "MathFunctions::greatest<TYPE_DECIMAL32>"],
    [102971, "greatest", "DECIMAL64", ["DECIMAL64", "..."], "MathFunctions::greatest<TYPE_DECIMAL64>"],
    [102972, "greatest", "DECIMAL128", ["DECIMAL128", "..."], "MathFunctions::greatest<TYPE_DECIMAL128>"],
    [10298, "greatest", "DATETIME", ["DATETIME", "..."], "MathFunctions::greatest<TYPE_DATETIME>"],
    [10299, "greatest", "VARCHAR", ["VARCHAR", "..."], "MathFunctions::greatest<TYPE_VARCHAR>"],

    [10300, "rand", "DOUBLE", [], "MathFunctions::rand", "MathFunctions::rand_prepare", "MathFunctions::rand_close"],
    [10301, "random", "DOUBLE", [], "MathFunctions::rand", "MathFunctions::rand_prepare", "MathFunctions::rand_close"],
    [10302, "rand", "DOUBLE", ["BIGINT"], "MathFunctions::rand_seed", "MathFunctions::rand_prepare",
     "MathFunctions::rand_close"],
    [10303, "random", "DOUBLE", ["BIGINT"], "MathFunctions::rand_seed", "MathFunctions::rand_prepare",
     "MathFunctions::rand_close"],

    [10311, "bin", "VARCHAR", ['BIGINT'], "MathFunctions::bin"],

    [10312, "hex", "VARCHAR", ['BIGINT'], "StringFunctions::hex_int"],
    [10313, "hex", "VARCHAR", ['VARCHAR'], "StringFunctions::hex_string"],
    [10314, "unhex", "VARCHAR", ['VARCHAR'], "StringFunctions::unhex"],
    [10315, "sm3", "VARCHAR", ['VARCHAR'], "StringFunctions::sm3"],

    [10320, "conv", "VARCHAR", ["BIGINT", "TINYINT", "TINYINT"], "MathFunctions::conv_int"],
    [10321, "conv", "VARCHAR", ["VARCHAR", "TINYINT", "TINYINT"], "MathFunctions::conv_string"],

    # 20xxx: bit functions
    [20010, 'bitand', 'TINYINT', ['TINYINT', 'TINYINT'], "BitFunctions::bitAnd<TYPE_TINYINT>"],
    [20011, 'bitand', 'SMALLINT', ['SMALLINT', 'SMALLINT'], "BitFunctions::bitAnd<TYPE_SMALLINT>"],
    [20012, 'bitand', 'INT', ['INT', 'INT'], "BitFunctions::bitAnd<TYPE_INT>"],
    [20013, 'bitand', 'BIGINT', ['BIGINT', 'BIGINT'], "BitFunctions::bitAnd<TYPE_BIGINT>"],
    [20014, 'bitand', 'LARGEINT', ['LARGEINT', 'LARGEINT'], "BitFunctions::bitAnd<TYPE_LARGEINT>"],

    [20020, 'bitor', 'TINYINT', ['TINYINT', 'TINYINT'], "BitFunctions::bitOr<TYPE_TINYINT>"],
    [20021, 'bitor', 'SMALLINT', ['SMALLINT', 'SMALLINT'], "BitFunctions::bitOr<TYPE_SMALLINT>"],
    [20022, 'bitor', 'INT', ['INT', 'INT'], "BitFunctions::bitOr<TYPE_INT>"],
    [20023, 'bitor', 'BIGINT', ['BIGINT', 'BIGINT'], "BitFunctions::bitOr<TYPE_BIGINT>"],
    [20024, 'bitor', 'LARGEINT', ['LARGEINT', 'LARGEINT'], "BitFunctions::bitOr<TYPE_LARGEINT>"],

    [20030, 'bitxor', 'TINYINT', ['TINYINT', 'TINYINT'], "BitFunctions::bitXor<TYPE_TINYINT>"],
    [20031, 'bitxor', 'SMALLINT', ['SMALLINT', 'SMALLINT'], "BitFunctions::bitXor<TYPE_SMALLINT>"],
    [20032, 'bitxor', 'INT', ['INT', 'INT'], "BitFunctions::bitXor<TYPE_INT>"],
    [20033, 'bitxor', 'BIGINT', ['BIGINT', 'BIGINT'], "BitFunctions::bitXor<TYPE_BIGINT>"],
    [20034, 'bitxor', 'LARGEINT', ['LARGEINT', 'LARGEINT'], "BitFunctions::bitXor<TYPE_LARGEINT>"],

    [20040, 'bitnot', 'TINYINT', ['TINYINT'], "BitFunctions::bitNot<TYPE_TINYINT>"],
    [20041, 'bitnot', 'SMALLINT', ['SMALLINT'], "BitFunctions::bitNot<TYPE_SMALLINT>"],
    [20042, 'bitnot', 'INT', ['INT'], "BitFunctions::bitNot<TYPE_INT>"],
    [20043, 'bitnot', 'BIGINT', ['BIGINT'], "BitFunctions::bitNot<TYPE_BIGINT>"],
    [20044, 'bitnot', 'LARGEINT', ['LARGEINT'], "BitFunctions::bitNot<TYPE_LARGEINT>"],

    # 30xxx: string functions
    [30010, 'substr', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::substring', 'StringFunctions::sub_str_prepare', 'StringFunctions::sub_str_close'],
    [30011, 'substr', 'VARCHAR', ['VARCHAR', 'INT', 'INT'], 'StringFunctions::substring', 'StringFunctions::sub_str_prepare', 'StringFunctions::sub_str_close'],
    [30012, 'substring', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::substring', 'StringFunctions::sub_str_prepare', 'StringFunctions::sub_str_close'],
    [30013, 'substring', 'VARCHAR', ['VARCHAR', 'INT', 'INT'], 'StringFunctions::substring', 'StringFunctions::sub_str_prepare', 'StringFunctions::sub_str_close'],

    [30020, 'left', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::left', 'StringFunctions::left_or_right_prepare', 'StringFunctions::left_or_right_close'],
    [30021, 'strleft', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::left', 'StringFunctions::left_or_right_prepare', 'StringFunctions::left_or_right_close'],

    [30030, 'right', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::right', 'StringFunctions::left_or_right_prepare', 'StringFunctions::left_or_right_close'],
    [30031, 'strright', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::right', 'StringFunctions::left_or_right_prepare', 'StringFunctions::left_or_right_close'],

    [30040, 'ends_with', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'StringFunctions::ends_with'],
    [30050, 'starts_with', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'StringFunctions::starts_with'],

    [30060, 'null_or_empty', 'BOOLEAN', ['VARCHAR'], 'StringFunctions::null_or_empty'],

    [30070, 'space', 'VARCHAR', ['INT'], 'StringFunctions::space'],
    [30080, 'repeat', 'VARCHAR', ['VARCHAR', 'INT'], 'StringFunctions::repeat'],

    [30090, 'lpad', 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], 'StringFunctions::lpad', 'StringFunctions::pad_prepare', 'StringFunctions::pad_close'],
    [30100, 'rpad', 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'], 'StringFunctions::rpad', 'StringFunctions::pad_prepare', 'StringFunctions::pad_close'],

    [30110, 'append_trailing_char_if_absent', 'VARCHAR', ['VARCHAR', 'VARCHAR'],
     'StringFunctions::append_trailing_char_if_absent'],

    [30120, 'length', 'INT', ['VARCHAR'], 'StringFunctions::length'],
    [30130, 'char_length', 'INT', ['VARCHAR'], 'StringFunctions::utf8_length'],
    [30131, 'character_length', 'INT', ['VARCHAR'], 'StringFunctions::utf8_length'],

    [30140, 'lower', 'VARCHAR', ['VARCHAR'], 'StringFunctions::lower'],
    [30141, 'lcase', 'VARCHAR', ['VARCHAR'], 'StringFunctions::lower'],

    [30150, 'upper', 'VARCHAR', ['VARCHAR'], 'StringFunctions::upper'],
    [30151, 'ucase', 'VARCHAR', ['VARCHAR'], 'StringFunctions::upper'],

    [30160, 'reverse', 'VARCHAR', ['VARCHAR'], 'StringFunctions::reverse'],
    [30170, 'trim', 'VARCHAR', ['VARCHAR'], 'StringFunctions::trim'],
    [30180, 'ltrim', 'VARCHAR', ['VARCHAR'], 'StringFunctions::ltrim'],
    [30190, 'rtrim', 'VARCHAR', ['VARCHAR'], 'StringFunctions::rtrim'],
    [30200, 'ascii', 'INT', ['VARCHAR'], 'StringFunctions::ascii'],
    [30500, 'char', 'VARCHAR', ['INT'], "StringFunctions::get_char"],
    [30210, 'instr', 'INT', ['VARCHAR', 'VARCHAR'], 'StringFunctions::instr'],
    [30220, 'locate', 'INT', ['VARCHAR', 'VARCHAR'], 'StringFunctions::locate'],
    [30221, 'locate', 'INT', ['VARCHAR', 'VARCHAR', 'INT'], 'StringFunctions::locate_pos'],

    [30250, 'concat', 'VARCHAR', ['VARCHAR', '...'], 'StringFunctions::concat', 'StringFunctions::concat_prepare', 'StringFunctions::concat_close'],

    [30260, 'concat_ws', 'VARCHAR', ['VARCHAR', 'VARCHAR', '...'], 'StringFunctions::concat_ws'],
    [30270, 'find_in_set', 'INT', ['VARCHAR', 'VARCHAR'], 'StringFunctions::find_in_set'],
    [30310, 'split_part', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'INT'], 'StringFunctions::split_part'],
    [30311, 'split', 'ARRAY_VARCHAR', ['VARCHAR', 'VARCHAR'], 'StringFunctions::split', 'StringFunctions::split_prepare', 'StringFunctions::split_close'],

    [30320, 'regexp_extract', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'BIGINT'], 'StringFunctions::regexp_extract',
     'StringFunctions::regexp_prepare', 'StringFunctions::regexp_close'],
    [30330, 'regexp_replace', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'], 'StringFunctions::regexp_replace',
     'StringFunctions::regexp_prepare', 'StringFunctions::regexp_close'],
    [30331, 'replace', 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'], 'StringFunctions::regexp_replace',
     'StringFunctions::regexp_prepare', 'StringFunctions::regexp_close'],
    [30400, "money_format", "VARCHAR", ["BIGINT"], "StringFunctions::money_format_bigint"],
    [30401, "money_format", "VARCHAR", ["LARGEINT"], "StringFunctions::money_format_largeint"],
    [30402, "money_format", "VARCHAR", ["DECIMALV2"], "StringFunctions::money_format_decimalv2val"],
    [304020, "money_format", "VARCHAR", ["DECIMAL32"], "StringFunctions::money_format_decimal<TYPE_DECIMAL32>"],
    [304021, "money_format", "VARCHAR", ["DECIMAL64"], "StringFunctions::money_format_decimal<TYPE_DECIMAL64>"],
    [304022, "money_format", "VARCHAR", ["DECIMAL128"], "StringFunctions::money_format_decimal<TYPE_DECIMAL128>"],
    [30403, "money_format", "VARCHAR", ["DOUBLE"], "StringFunctions::money_format_double"],

    [30410, 'parse_url', 'VARCHAR', ['VARCHAR', 'VARCHAR'], 'StringFunctions::parse_url',
     'StringFunctions::parse_url_prepare', 'StringFunctions::parse_url_close'],

    # 50xxx: timestamp functions
    [50010, 'year', 'INT', ['DATETIME'], 'TimeFunctions::year'],
    [50020, 'month', 'INT', ['DATETIME'], 'TimeFunctions::month'],
    [50030, 'quarter', 'INT', ['DATETIME'], 'TimeFunctions::quarter'],
    [50040, 'dayofweek', 'INT', ['DATETIME'], 'TimeFunctions::day_of_week'],
    [50050, 'to_date', 'DATE', ['DATETIME'], 'TimeFunctions::to_date'],
    [50051, 'date', 'DATE', ['DATETIME'], 'TimeFunctions::to_date'],
    [50060, 'dayofmonth', 'INT', ['DATETIME'], 'TimeFunctions::day'],
    [50061, 'day', 'INT', ['DATETIME'], 'TimeFunctions::day'],
    [50062, 'dayofyear', 'INT', ['DATETIME'], 'TimeFunctions::day_of_year'],
    [50063, 'weekofyear', 'INT', ['DATETIME'], 'TimeFunctions::week_of_year'],

    [50070, 'hour', 'INT', ['DATETIME'], 'TimeFunctions::hour'],
    [50080, 'minute', 'INT', ['DATETIME'], 'TimeFunctions::minute'],
    [50090, 'second', 'INT', ['DATETIME'], 'TimeFunctions::second'],

    [50110, 'years_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::years_add'],
    [50111, 'years_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::years_sub'],
    [50120, 'months_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::months_add'],
    [50121, 'months_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::months_sub'],
    [50122, 'add_months', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::months_add'],
    [50130, 'weeks_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::weeks_add'],
    [50131, 'weeks_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::weeks_sub'],
    [50140, 'days_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::days_add'],
    [50141, 'days_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::days_sub'],

    [50142, 'date_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::days_add'],
    [50143, 'date_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::days_sub'],

    [50144, 'adddate', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::days_add'],
    [50145, 'subdate', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::days_sub'],

    [50150, 'hours_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::hours_add'],
    [50151, 'hours_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::hours_sub'],
    [50160, 'minutes_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::minutes_add'],
    [50161, 'minutes_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::minutes_sub'],
    [50170, 'seconds_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::seconds_add'],
    [50171, 'seconds_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::seconds_sub'],
    [50180, 'microseconds_add', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::micros_add'],
    [50181, 'microseconds_sub', 'DATETIME', ['DATETIME', 'INT'], 'TimeFunctions::micros_sub'],
    [50190, 'years_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::years_diff'],
    [50191, 'months_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::months_diff'],
    [50192, 'weeks_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::weeks_diff'],
    [50193, 'days_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::days_diff'],
    [50194, 'hours_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::hours_diff'],
    [50195, 'minutes_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::minutes_diff'],
    [50196, 'seconds_diff', 'BIGINT', ['DATETIME', 'DATETIME'], 'TimeFunctions::seconds_diff'],
    [50197, 'datediff', 'INT', ['DATETIME', 'DATETIME'], 'TimeFunctions::date_diff'],
    [50198, 'timediff', 'TIME', ['DATETIME', 'DATETIME'], 'TimeFunctions::time_diff'],
    [50200, 'now', 'DATETIME', [], 'TimeFunctions::now'],
    [50201, 'current_timestamp', 'DATETIME', [], 'TimeFunctions::now'],
    [50202, 'localtime', 'DATETIME', [], 'TimeFunctions::now'],
    [50203, 'localtimestamp', 'DATETIME', [], 'TimeFunctions::now'],
    [50210, 'curtime', 'TIME', [], 'TimeFunctions::curtime'],
    [50211, 'current_time', 'TIME', [], 'TimeFunctions::curtime'],
    [50220, 'curdate', 'DATE', [], 'TimeFunctions::curdate'],
    [50221, 'current_date', 'DATE', [], 'TimeFunctions::curdate'],
    [50230, 'from_days', 'DATE', ['INT'], 'TimeFunctions::from_days'],
    [50231, 'to_days', 'INT', ['DATE'], 'TimeFunctions::to_days'],
    [50240, 'str_to_date', 'DATETIME', ['VARCHAR', 'VARCHAR'], 'TimeFunctions::str_to_date', 'TimeFunctions::str_to_date_prepare', 'TimeFunctions::str_to_date_close'],
    [50241, 'date_format', 'VARCHAR', ['DATETIME', 'VARCHAR'], 'TimeFunctions::datetime_format',
     'TimeFunctions::format_prepare', 'TimeFunctions::format_close'],
    [50242, 'date_format', 'VARCHAR', ['DATE', 'VARCHAR'], 'TimeFunctions::date_format',
     'TimeFunctions::format_prepare', 'TimeFunctions::format_close'],
    [50250, 'time_to_sec', 'BIGINT', ['TIME'], 'TimeFunctions::time_to_sec'],

    [50300, 'unix_timestamp', 'INT', [], 'TimeFunctions::to_unix_for_now'],
    [50301, 'unix_timestamp', 'INT', ['DATETIME'], 'TimeFunctions::to_unix_from_datetime'],
    [50302, 'unix_timestamp', 'INT', ['DATE'], 'TimeFunctions::to_unix_from_date'],
    [50303, 'unix_timestamp', 'INT', ['VARCHAR', 'VARCHAR'], 'TimeFunctions::to_unix_from_datetime_with_format'],
    [50304, 'from_unixtime', 'VARCHAR', ['INT'], 'TimeFunctions::from_unix_to_datetime'],
    [50305, 'from_unixtime', 'VARCHAR', ['INT', 'VARCHAR'], 'TimeFunctions::from_unix_to_datetime_with_format', 'TimeFunctions::from_unix_prepare', 'TimeFunctions::from_unix_close'],

    [50310, 'dayname', 'VARCHAR', ['DATETIME'], 'TimeFunctions::day_name'],
    [50311, 'monthname', 'VARCHAR', ['DATETIME'], 'TimeFunctions::month_name'],
    [50320, 'convert_tz', 'DATETIME', ['DATETIME', 'VARCHAR', 'VARCHAR'], 'TimeFunctions::convert_tz', 'TimeFunctions::convert_tz_prepare', 'TimeFunctions::convert_tz_close'],
    [50330, 'utc_timestamp', 'DATETIME', [], 'TimeFunctions::utc_timestamp'],
    [50340, 'date_trunc', 'DATETIME', ['VARCHAR', 'DATETIME'], 'TimeFunctions::datetime_trunc', 'TimeFunctions::datetime_trunc_prepare', 'TimeFunctions::datetime_trunc_close'],
    [50350, 'date_trunc', 'DATE', ['VARCHAR', 'DATE'], 'TimeFunctions::date_trunc', 'TimeFunctions::date_trunc_prepare', 'TimeFunctions::date_trunc_close'],
    [50360, 'timestamp', 'DATETIME', ['DATETIME'], 'TimeFunctions::timestamp'],

    # 60xxx: like predicate
    # important ref: LikePredicate.java, must keep name equals LikePredicate.Operator
    [60010, 'LIKE', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'LikePredicate::like', 'LikePredicate::like_prepare',
     'LikePredicate::like_close'],
    [60020, 'REGEXP', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'LikePredicate::regex', 'LikePredicate::regex_prepare',
     'LikePredicate::regex_close'],

    # 70xxx: condition functions
    # In fact, condition function will use condition express, not function. There just support
    # a function name for FE.
    # Why use express and not functions? I think one of the reasons is performance, we need to
    # execute all the children expressions ahead of time in function_call_expr, but condition
    # expressions may not need execute all children expressions if the condition be true ahead
    # of time
    [70100, 'if', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], 'nullptr'],
    [70101, 'if', 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], 'nullptr'],
    [70102, 'if', 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], 'nullptr'],
    [70103, 'if', 'INT', ['BOOLEAN', 'INT', 'INT'], 'nullptr'],
    [70104, 'if', 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], 'nullptr'],
    [70105, 'if', 'LARGEINT', ['BOOLEAN', 'LARGEINT', 'LARGEINT'], 'nullptr'],
    [70106, 'if', 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], 'nullptr'],
    [70107, 'if', 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], 'nullptr'],
    [70108, 'if', 'DATETIME', ['BOOLEAN', 'DATETIME', 'DATETIME'], 'nullptr'],
    [70109, 'if', 'DATE', ['BOOLEAN', 'DATE', 'DATE'], 'nullptr'],
    [70110, 'if', 'DECIMALV2', ['BOOLEAN', 'DECIMALV2', 'DECIMALV2'], 'nullptr'],
    [701100, 'if', 'DECIMAL32', ['BOOLEAN', 'DECIMAL32', 'DECIMAL32'], 'nullptr'],
    [701101, 'if', 'DECIMAL64', ['BOOLEAN', 'DECIMAL64', 'DECIMAL64'], 'nullptr'],
    [701102, 'if', 'DECIMAL128', ['BOOLEAN', 'DECIMAL128', 'DECIMAL128'], 'nullptr'],
    [70111, 'if', 'VARCHAR', ['BOOLEAN', 'VARCHAR', 'VARCHAR'], 'nullptr'],
    [70112, 'if', 'BITMAP', ['BOOLEAN', 'BITMAP', 'BITMAP'], 'nullptr'],
    [70113, 'if', 'PERCENTILE', ['BOOLEAN', 'PERCENTILE', 'PERCENTILE'], 'nullptr'],
    [70114, 'if', 'HLL', ['BOOLEAN', 'HLL', 'HLL'], 'nullptr'],

    [70200, 'ifnull', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'nullptr'],
    [70201, 'ifnull', 'TINYINT', ['TINYINT', 'TINYINT'], 'nullptr'],
    [70202, 'ifnull', 'SMALLINT', ['SMALLINT', 'SMALLINT'], 'nullptr'],
    [70203, 'ifnull', 'INT', ['INT', 'INT'], 'nullptr'],
    [70204, 'ifnull', 'BIGINT', ['BIGINT', 'BIGINT'], 'nullptr'],
    [70205, 'ifnull', 'LARGEINT', ['LARGEINT', 'LARGEINT'], 'nullptr'],
    [70206, 'ifnull', 'FLOAT', ['FLOAT', 'FLOAT'], 'nullptr'],
    [70207, 'ifnull', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'nullptr'],
    [70208, 'ifnull', 'DATE', ['DATE', 'DATE'], 'nullptr'],
    [70209, 'ifnull', 'DATETIME', ['DATETIME', 'DATETIME'], 'nullptr'],
    [70210, 'ifnull', 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], 'nullptr'],
    [702100, 'ifnull', 'DECIMAL32', ['DECIMAL32', 'DECIMAL32'], 'nullptr'],
    [702101, 'ifnull', 'DECIMAL64', ['DECIMAL64', 'DECIMAL64'], 'nullptr'],
    [702102, 'ifnull', 'DECIMAL128', ['DECIMAL128', 'DECIMAL128'], 'nullptr'],
    [70211, 'ifnull', 'VARCHAR', ['VARCHAR', 'VARCHAR'], 'nullptr'],
    [70212, 'ifnull', 'BITMAP', ['BITMAP', 'BITMAP'], 'nullptr'],
    [70213, 'ifnull', 'PERCENTILE', ['PERCENTILE', 'PERCENTILE'], 'nullptr'],
    [70214, 'ifnull', 'HLL', ['HLL', 'HLL'], 'nullptr'],

    [70300, 'nullif', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'nullptr'],
    [70301, 'nullif', 'TINYINT', ['TINYINT', 'TINYINT'], 'nullptr'],
    [70302, 'nullif', 'SMALLINT', ['SMALLINT', 'SMALLINT'], 'nullptr'],
    [70303, 'nullif', 'INT', ['INT', 'INT'], 'nullptr'],
    [70304, 'nullif', 'BIGINT', ['BIGINT', 'BIGINT'], 'nullptr'],
    [70305, 'nullif', 'LARGEINT', ['LARGEINT', 'LARGEINT'], 'nullptr'],
    [70306, 'nullif', 'FLOAT', ['FLOAT', 'FLOAT'], 'nullptr'],
    [70307, 'nullif', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'nullptr'],
    [70308, 'nullif', 'DATE', ['DATE', 'DATE'], 'nullptr'],
    [70309, 'nullif', 'DATETIME', ['DATETIME', 'DATETIME'], 'nullptr'],
    [70310, 'nullif', 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], 'nullptr'],
    [703100, 'nullif', 'DECIMAL32', ['DECIMAL32', 'DECIMAL32'], 'nullptr'],
    [703101, 'nullif', 'DECIMAL64', ['DECIMAL64', 'DECIMAL64'], 'nullptr'],
    [703102, 'nullif', 'DECIMAL128', ['DECIMAL128', 'DECIMAL128'], 'nullptr'],
    [70311, 'nullif', 'VARCHAR', ['VARCHAR', 'VARCHAR'], 'nullptr'],
    [70312, 'nullif', 'BITMAP', ['BITMAP', 'BITMAP'], 'nullptr'],
    [70313, 'nullif', 'PERCENTILE', ['PERCENTILE', 'PERCENTILE'], 'nullptr'],
    [70314, 'nullif', 'HLL', ['HLL', 'HLL'], 'nullptr'],

    [70400, 'coalesce', 'BOOLEAN', ['BOOLEAN', '...'], 'nullptr'],
    [70401, 'coalesce', 'TINYINT', ['TINYINT', '...'], 'nullptr'],
    [70402, 'coalesce', 'SMALLINT', ['SMALLINT', '...'], 'nullptr'],
    [70403, 'coalesce', 'INT', ['INT', '...'], 'nullptr'],
    [70404, 'coalesce', 'BIGINT', ['BIGINT', '...'], 'nullptr'],
    [70405, 'coalesce', 'LARGEINT', ['LARGEINT', '...'], 'nullptr'],
    [70406, 'coalesce', 'FLOAT', ['FLOAT', '...'], 'nullptr'],
    [70407, 'coalesce', 'DOUBLE', ['DOUBLE', '...'], 'nullptr'],
    [70408, 'coalesce', 'DATETIME', ['DATETIME', '...'], 'nullptr'],
    [70409, 'coalesce', 'DATE', ['DATE', '...'], 'nullptr'],
    [70410, 'coalesce', 'DECIMALV2', ['DECIMALV2', '...'], 'nullptr'],
    [704100, 'coalesce', 'DECIMAL32', ['DECIMAL32', '...'], 'nullptr'],
    [704101, 'coalesce', 'DECIMAL64', ['DECIMAL64', '...'], 'nullptr'],
    [704102, 'coalesce', 'DECIMAL128', ['DECIMAL128', '...'], 'nullptr'],
    [70411, 'coalesce', 'VARCHAR', ['VARCHAR', '...'], 'nullptr'],
    [70412, 'coalesce', 'BITMAP', ['BITMAP', '...'], 'nullptr'],
    [70413, 'coalesce', 'PERCENTILE', ['PERCENTILE', '...'], 'nullptr'],
    [70414, 'coalesce', 'HLL', ['HLL', '...'], 'nullptr'],

    [70415, 'esquery', 'BOOLEAN', ['VARCHAR', 'VARCHAR'], 'ESFunctions::match'],

    # hyperloglog function
    [80010, 'hll_cardinality', 'BIGINT', ['HLL'], 'HyperloglogFunction::hll_cardinality'],
    [80011, 'hll_cardinality', 'BIGINT', ['VARCHAR'], 'HyperloglogFunction::hll_cardinality_from_string'],
    [80020, 'hll_hash', 'HLL', ['VARCHAR'], 'HyperloglogFunction::hll_hash'],
    [80030, 'hll_empty', 'HLL', [], 'HyperloglogFunction::hll_empty'],

    # bitmap function
    [90010, 'to_bitmap', 'BITMAP', ['VARCHAR'], 'BitmapFunctions::to_bitmap'],
    [90020, 'bitmap_hash', 'BITMAP', ['VARCHAR'], 'BitmapFunctions::bitmap_hash'],
    [90030, 'bitmap_count', 'BIGINT', ['BITMAP'], 'BitmapFunctions::bitmap_count'],
    [90040, 'bitmap_empty', 'BITMAP', [], 'BitmapFunctions::bitmap_empty'],
    [90050, 'bitmap_or', 'BITMAP', ['BITMAP', 'BITMAP'], 'BitmapFunctions::bitmap_or'],
    [90060, 'bitmap_and', 'BITMAP', ['BITMAP', 'BITMAP'], 'BitmapFunctions::bitmap_and'],
    [90070, 'bitmap_to_string', 'VARCHAR', ['BITMAP'], 'BitmapFunctions::bitmap_to_string'],
    [90080, 'bitmap_from_string', 'BITMAP', ['VARCHAR'], 'BitmapFunctions::bitmap_from_string'],
    [90090, 'bitmap_contains', 'BOOLEAN', ['BITMAP', 'BIGINT'], 'BitmapFunctions::bitmap_contains'],
    [90100, 'bitmap_has_any', 'BOOLEAN', ['BITMAP', 'BITMAP'], 'BitmapFunctions::bitmap_has_any'],
    [90200, 'bitmap_andnot', 'BITMAP', ['BITMAP', 'BITMAP'], 'BitmapFunctions::bitmap_andnot'],
    [90300, 'bitmap_xor', 'BITMAP', ['BITMAP', 'BITMAP'], 'BitmapFunctions::bitmap_xor'],
    [90400, 'bitmap_remove', 'BITMAP', ['BITMAP', 'BIGINT'], 'BitmapFunctions::bitmap_remove'],
    [90500, 'bitmap_to_array', 'ARRAY_BIGINT', ['BITMAP'], 'BitmapFunctions::bitmap_to_array'],

    # hash function
    [100010, 'murmur_hash3_32', 'INT', ['VARCHAR', '...'], 'HashFunctions::murmur_hash3_32'],

    # Utility functions
    [100011, 'sleep', 'BOOLEAN', ['INT'], "UtilityFunctions::sleep"],
    [100012, 'version', 'VARCHAR', [], "UtilityFunctions::version"],
    [100013, 'current_version', 'VARCHAR', [], "UtilityFunctions::current_version"],
    [100014, 'last_query_id', 'VARCHAR', [], "UtilityFunctions::last_query_id"],
    [100015, 'uuid', 'VARCHAR', [], "UtilityFunctions::uuid"],

    # json function
    [110000, "get_json_int", "INT", ["VARCHAR", "VARCHAR"], "JsonFunctions::get_json_int",
     "JsonFunctions::json_path_prepare", "JsonFunctions::json_path_close"],
    [110001, "get_json_double", "DOUBLE", ["VARCHAR", "VARCHAR"], "JsonFunctions::get_json_double",
     "JsonFunctions::json_path_prepare", "JsonFunctions::json_path_close"],
    [110002, "get_json_string", "VARCHAR", ["VARCHAR", "VARCHAR"], "JsonFunctions::get_json_string",
     "JsonFunctions::json_path_prepare", "JsonFunctions::json_path_close"],

    # aes and base64 function
    [120100, "aes_encrypt", "VARCHAR", ["VARCHAR", "VARCHAR"], "EncryptionFunctions::aes_encrypt"],
    [120110, "aes_decrypt", "VARCHAR", ["VARCHAR", "VARCHAR"], "EncryptionFunctions::aes_decrypt"],
    [120120, "from_base64", "VARCHAR", ["VARCHAR"], "EncryptionFunctions::from_base64"],
    [120130, "to_base64", "VARCHAR", ["VARCHAR"], "EncryptionFunctions::to_base64"],
    [120140, "md5", "VARCHAR", ["VARCHAR"], "EncryptionFunctions::md5"],
    [120150, "md5sum", "VARCHAR", ["VARCHAR", "..."], "EncryptionFunctions::md5sum"],

      # geo function
    [120000, "ST_Point", "VARCHAR", ["DOUBLE", "DOUBLE"], "GeoFunctions::st_point"],
    [120001, "ST_X", "DOUBLE", ["VARCHAR"], "GeoFunctions::st_x"],
    [120002, "ST_Y", "DOUBLE", ["VARCHAR"], "GeoFunctions::st_y"],
    [120003, "ST_Distance_Sphere", "DOUBLE", ["DOUBLE", "DOUBLE", "DOUBLE", "DOUBLE"], "GeoFunctions::st_distance_sphere"],
    [120004, "ST_AsText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_as_wkt"],
    [120005, "ST_AsWKT", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_as_wkt"],
    [120006, "ST_GeometryFromText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_from_wkt", "GeoFunctions::st_from_wkt_prepare", "GeoFunctions::st_from_wkt_close"],
    [120007, "ST_GeomFromText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_from_wkt", "GeoFunctions::st_from_wkt_prepare", "GeoFunctions::st_from_wkt_close"],
    [120008, "ST_LineFromText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_line", "GeoFunctions::st_line_prepare", "GeoFunctions::st_from_wkt_close"],
    [120009, "ST_LineStringFromText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_line", "GeoFunctions::st_line_prepare", "GeoFunctions::st_from_wkt_close"],
    [120010, "ST_Polygon", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_polygon", "GeoFunctions::st_polygon_prepare", "GeoFunctions::st_from_wkt_close"],
    [120011, "ST_PolyFromText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_polygon", "GeoFunctions::st_polygon_prepare", "GeoFunctions::st_from_wkt_close"],
    [120012, "ST_PolygonFromText", "VARCHAR", ["VARCHAR"], "GeoFunctions::st_polygon", "GeoFunctions::st_polygon_prepare", "GeoFunctions::st_from_wkt_close"],
    [120013, "ST_Circle", "VARCHAR", ["DOUBLE", "DOUBLE", "DOUBLE"], "GeoFunctions::st_circle", "GeoFunctions::st_circle_prepare", "GeoFunctions::st_from_wkt_close"],
    [120014, "ST_Contains", "BOOLEAN", ["VARCHAR", "VARCHAR"], "GeoFunctions::st_contains", "GeoFunctions::st_contains_prepare", "GeoFunctions::st_contains_close"],

    # percentile function
    [130000, 'percentile_hash', 'PERCENTILE', ['DOUBLE'], 'PercentileFunctions::percentile_hash'],
    [130001, 'percentile_empty', 'PERCENTILE', [], 'PercentileFunctions::percentile_empty'],
    [130002, 'percentile_approx_raw', 'DOUBLE', ['PERCENTILE', 'DOUBLE'], 'PercentileFunctions::percentile_approx_raw'],

    [140000, 'grouping_id', 'BIGINT', ['BIGINT'], 'GroupingSetsFunctions::grouping_id'],
    [140001, 'grouping', 'BIGINT', ['BIGINT'], 'GroupingSetsFunctions::grouping'],

    [150000, 'array_length', 'INT', ['ANY_ARRAY'], 'ArrayFunctions::array_length'],
    [150001, 'array_append', 'ANY_ARRAY', ['ANY_ARRAY', 'ANY_ELEMENT'], 'ArrayFunctions::array_append'],
    [150002, 'array_contains', 'BOOLEAN', ['ANY_ARRAY', 'ANY_ELEMENT'], 'ArrayFunctions::array_contains'],

    #sum    
    [150003, 'array_sum', 'BIGINT', ['ARRAY_BOOLEAN'], 'ArrayFunctions::array_sum_boolean'],
    [150004, 'array_sum', 'BIGINT', ['ARRAY_TINYINT'], 'ArrayFunctions::array_sum_tinyint'],
    [150005, 'array_sum', 'BIGINT', ['ARRAY_SMALLINT'], 'ArrayFunctions::array_sum_smallint'],
    [150006, 'array_sum', 'BIGINT', ['ARRAY_INT'], 'ArrayFunctions::array_sum_int'],
    [150007, 'array_sum', 'BIGINT', ['ARRAY_BIGINT'], 'ArrayFunctions::array_sum_bigint'],
    [150008, 'array_sum', 'LARGEINT', ['ARRAY_LARGEINT'], 'ArrayFunctions::array_sum_largeint'],
    [150009, 'array_sum', 'DOUBLE', ['ARRAY_FLOAT'], 'ArrayFunctions::array_sum_float'],
    [150010, 'array_sum', 'DOUBLE', ['ARRAY_DOUBLE'], 'ArrayFunctions::array_sum_double'],
    [150011, 'array_sum', 'DECIMALV2', ['ARRAY_DECIMALV2'], 'ArrayFunctions::array_sum_decimalv2'],
    #[150012, 'array_sum', 'DECIMAL64', ['ARRAY_DECIMAL32'], 'ArrayFunctions::array_sum'],
    #[150013, 'array_sum', 'DECIMAL64', ['ARRAY_DECIMAL64'], 'ArrayFunctions::array_sum'],
    #[150014, 'array_sum', 'DECIMAL128', ['ARRAY_DECIMAL128'], 'ArrayFunctions::array_sum'],

    #avg
    [150023, 'array_avg', 'DOUBLE', ['ARRAY_BOOLEAN'], 'ArrayFunctions::array_avg_boolean'],
    [150024, 'array_avg', 'DOUBLE', ['ARRAY_TINYINT'], 'ArrayFunctions::array_avg_tinyint'],
    [150025, 'array_avg', 'DOUBLE', ['ARRAY_SMALLINT'], 'ArrayFunctions::array_avg_smallint'],
    [150026, 'array_avg', 'DOUBLE', ['ARRAY_INT'], 'ArrayFunctions::array_avg_int'],
    [150027, 'array_avg', 'DOUBLE', ['ARRAY_BIGINT'], 'ArrayFunctions::array_avg_bigint'],
    [150028, 'array_avg', 'DOUBLE', ['ARRAY_LARGEINT'], 'ArrayFunctions::array_avg_largeint'],
    [150029, 'array_avg', 'DOUBLE', ['ARRAY_FLOAT'], 'ArrayFunctions::array_avg_float'],
    [150030, 'array_avg', 'DOUBLE', ['ARRAY_DOUBLE'], 'ArrayFunctions::array_avg_double'],
    [150031, 'array_avg', 'DECIMALV2', ['ARRAY_DECIMALV2'], 'ArrayFunctions::array_avg_decimalv2'],
    #[150032, 'array_avg', 'DATE', ['ARRAY_DATE'], 'ArrayFunctions::array_avg_date'],
    #[150033, 'array_avg', 'DATETIME', ['ARRAY_DATETIME'], 'ArrayFunctions::array_avg_datetime'],
    #[150012, 'array_avg', 'DECIMAL64', ['ARRAY_DECIMAL32'], 'ArrayFunctions::array_avg'],
    #[150013, 'array_avg', 'DECIMAL64', ['ARRAY_DECIMAL64'], 'ArrayFunctions::array_avg'],
    #[150014, 'array_avg', 'DECIMAL128', ['ARRAY_DECIMAL128'], 'ArrayFunctions::array_avg'],

    #min
    [150043, 'array_min', 'BOOLEAN', ['ARRAY_BOOLEAN'], 'ArrayFunctions::array_min_boolean'],
    [150044, 'array_min', 'TINYINT', ['ARRAY_TINYINT'], 'ArrayFunctions::array_min_tinyint'],
    [150045, 'array_min', 'SMALLINT', ['ARRAY_SMALLINT'], 'ArrayFunctions::array_min_smallint'],
    [150046, 'array_min', 'INT', ['ARRAY_INT'], 'ArrayFunctions::array_min_int'],
    [150047, 'array_min', 'BIGINT', ['ARRAY_BIGINT'], 'ArrayFunctions::array_min_bigint'],
    [150048, 'array_min', 'LARGEINT', ['ARRAY_LARGEINT'], 'ArrayFunctions::array_min_largeint'],
    [150049, 'array_min', 'FLOAT', ['ARRAY_FLOAT'], 'ArrayFunctions::array_min_float'],
    [150050, 'array_min', 'DOUBLE', ['ARRAY_DOUBLE'], 'ArrayFunctions::array_min_double'],
    [150051, 'array_min', 'DECIMALV2', ['ARRAY_DECIMALV2'], 'ArrayFunctions::array_min_decimalv2'],
    [150052, 'array_min', 'DATE', ['ARRAY_DATE'], 'ArrayFunctions::array_min_date'],
    [150053, 'array_min', 'DATETIME', ['ARRAY_DATETIME'], 'ArrayFunctions::array_min_datetime'],
    [150054, 'array_min', 'VARCHAR', ['ARRAY_VARCHAR'], 'ArrayFunctions::array_min_varchar'],
    #[150012, 'array_min', 'DECIMAL64', ['ARRAY_DECIMAL32'], 'ArrayFunctions::array_min'],
    #[150013, 'array_min', 'DECIMAL64', ['ARRAY_DECIMAL64'], 'ArrayFunctions::array_min'],
    #[150014, 'array_min', 'DECIMAL128', ['ARRAY_DECIMAL128'], 'ArrayFunctions::array_min'],

    #max
    [150063, 'array_max', 'BOOLEAN', ['ARRAY_BOOLEAN'], 'ArrayFunctions::array_max_boolean'],
    [150064, 'array_max', 'TINYINT', ['ARRAY_TINYINT'], 'ArrayFunctions::array_max_tinyint'],
    [150065, 'array_max', 'SMALLINT', ['ARRAY_SMALLINT'], 'ArrayFunctions::array_max_smallint'],
    [150066, 'array_max', 'INT', ['ARRAY_INT'], 'ArrayFunctions::array_max_int'],
    [150067, 'array_max', 'BIGINT', ['ARRAY_BIGINT'], 'ArrayFunctions::array_max_bigint'],
    [150068, 'array_max', 'LARGEINT', ['ARRAY_LARGEINT'], 'ArrayFunctions::array_max_largeint'],
    [150069, 'array_max', 'FLOAT', ['ARRAY_FLOAT'], 'ArrayFunctions::array_max_float'],
    [150070, 'array_max', 'DOUBLE', ['ARRAY_DOUBLE'], 'ArrayFunctions::array_max_double'],
    [150071, 'array_max', 'DECIMALV2', ['ARRAY_DECIMALV2'], 'ArrayFunctions::array_max_decimalv2'],
    [150072, 'array_max', 'DATE', ['ARRAY_DATE'], 'ArrayFunctions::array_max_date'],
    [150073, 'array_max', 'DATETIME', ['ARRAY_DATETIME'], 'ArrayFunctions::array_max_datetime'],
    [150074, 'array_max', 'VARCHAR', ['ARRAY_VARCHAR'], 'ArrayFunctions::array_max_varchar'],
    #[150012, 'array_max', 'DECIMAL64', ['ARRAY_DECIMAL32'], 'ArrayFunctions::array_max'],
    #[150013, 'array_max', 'DECIMAL64', ['ARRAY_DECIMAL64'], 'ArrayFunctions::array_max'],
    #[150014, 'array_max', 'DECIMAL128', ['ARRAY_DECIMAL128'], 'ArrayFunctions::array_max'],
]
