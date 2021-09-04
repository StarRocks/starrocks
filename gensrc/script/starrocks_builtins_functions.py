#!/usr/bin/env python
# encoding: utf-8

# This file is made available under Elastic License 2.0
# This file is based on code available under the Apache license here:
#   https://github.com/apache/incubator-doris/blob/master/gensrc/script/doris_builtins_functions.py

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This is a list of all the functions that are not auto-generated.
# It contains all the meta data that describes the function.

# The format is:
#   [sql aliases], <return_type>, [<args>], <backend symbol>,
# With an optional
#   <prepare symbol>, <close symbol>
#
# 'sql aliases' are the function names that can be used from sql. There must be at least
# one per function.
#
# The symbol can be empty for functions that are not yet implemented or are special-cased
# in Expr::CreateExpr() (i.e., functions that are implemented via a custom Expr class
# rather than a single function).
visible_functions = [
    # Bit and Byte functions
    # For functions corresponding to builtin operators, we can reuse the implementations
    [['bitand'], 'TINYINT', ['TINYINT', 'TINYINT'],
        '_ZN9starrocks9Operators32bitand_tiny_int_val_tiny_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitand'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
        '_ZN9starrocks9Operators34bitand_small_int_val_small_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitand'], 'INT', ['INT', 'INT'],
        '_ZN9starrocks9Operators22bitand_int_val_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitand'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN9starrocks9Operators30bitand_big_int_val_big_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitand'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN9starrocks9Operators34bitand_large_int_val_large_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitor'], 'TINYINT', ['TINYINT', 'TINYINT'],
        '_ZN9starrocks9Operators31bitor_tiny_int_val_tiny_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
        '_ZN9starrocks9Operators33bitor_small_int_val_small_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitor'], 'INT', ['INT', 'INT'],
        '_ZN9starrocks9Operators21bitor_int_val_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitor'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN9starrocks9Operators29bitor_big_int_val_big_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN9starrocks9Operators33bitor_large_int_val_large_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitxor'], 'TINYINT', ['TINYINT', 'TINYINT'],
        '_ZN9starrocks9Operators32bitxor_tiny_int_val_tiny_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['bitxor'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
        '_ZN9starrocks9Operators34bitxor_small_int_val_small_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['bitxor'], 'INT', ['INT', 'INT'],
        '_ZN9starrocks9Operators22bitxor_int_val_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_6IntValES6_'],
    [['bitxor'], 'BIGINT', ['BIGINT', 'BIGINT'],
        '_ZN9starrocks9Operators30bitxor_big_int_val_big_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_9BigIntValES6_'],
    [['bitxor'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
        '_ZN9starrocks9Operators34bitxor_large_int_val_large_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['bitnot'], 'TINYINT', ['TINYINT'],
        '_ZN9starrocks9Operators19bitnot_tiny_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_10TinyIntValE'],
    [['bitnot'], 'SMALLINT', ['SMALLINT'],
        '_ZN9starrocks9Operators20bitnot_small_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11SmallIntValE'],
    [['bitnot'], 'INT', ['INT'],
        '_ZN9starrocks9Operators14bitnot_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_6IntValE'],
    [['bitnot'], 'BIGINT', ['BIGINT'],
        '_ZN9starrocks9Operators18bitnot_big_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_9BigIntValE'],
    [['bitnot'], 'LARGEINT', ['LARGEINT'],
        '_ZN9starrocks9Operators20bitnot_large_int_valEPN13starrocks_udf'
        '15FunctionContextERKNS1_11LargeIntValE'],

    # Timestamp functions
    [['unix_timestamp'], 'INT', [],
        '_ZN9starrocks18TimestampFunctions7to_unixEPN13starrocks_udf15FunctionContextE'],
    [['unix_timestamp'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions7to_unixEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['unix_timestamp'], 'INT', ['DATE'],
        '_ZN9starrocks18TimestampFunctions7to_unixEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['unix_timestamp'], 'INT', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks18TimestampFunctions7to_unixEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['from_unixtime'], 'VARCHAR', ['INT'],
        '_ZN9starrocks18TimestampFunctions9from_unixEPN13starrocks_udf15FunctionContextERKNS1_6IntValE'],
    [['from_unixtime'], 'VARCHAR', ['INT', 'VARCHAR'],
        '_ZN9starrocks18TimestampFunctions9from_unixEPN13starrocks_udf'
        '15FunctionContextERKNS1_6IntValERKNS1_9StringValE',
        '_ZN9starrocks18TimestampFunctions14format_prepareEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks18TimestampFunctions12format_closeEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE'],
    [['now', 'current_timestamp', 'localtime', 'localtimestamp'], 'DATETIME', [],
        '_ZN9starrocks18TimestampFunctions3nowEPN13starrocks_udf15FunctionContextE'],
    [['curtime', 'current_time'], 'TIME', [],
        '_ZN9starrocks18TimestampFunctions7curtimeEPN13starrocks_udf15FunctionContextE'],
    [['curdate', 'current_date'], 'DATE', [],
        '_ZN9starrocks18TimestampFunctions7curdateEPN13starrocks_udf15FunctionContextE'],
    [['utc_timestamp'], 'DATETIME', [],
        '_ZN9starrocks18TimestampFunctions13utc_timestampEPN13starrocks_udf15FunctionContextE'],
    [['timestamp'], 'DATETIME', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions9timestampEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['from_days'], 'DATE', ['INT'],
        '_ZN9starrocks18TimestampFunctions9from_daysEPN13starrocks_udf15FunctionContextERKNS1_6IntValE'],
    [['to_days'], 'INT', ['DATE'],
        '_ZN9starrocks18TimestampFunctions7to_daysEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['year'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions4yearEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['month'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions5monthEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['quarter'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions7quarterEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['dayofweek'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions11day_of_weekEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['day', 'dayofmonth'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions12day_of_monthEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['dayofyear'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions11day_of_yearEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['weekofyear'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions12week_of_yearEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['hour'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions4hourEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['minute'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions6minuteEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],
    [['second'], 'INT', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions6secondEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['years_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions9years_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['years_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions9years_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['months_add', 'add_months'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions10months_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['months_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions10months_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['weeks_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions9weeks_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['weeks_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions9weeks_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['days_add', 'date_add', 'adddate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions8days_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['days_sub', 'date_sub', 'subdate'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions8days_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['hours_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions9hours_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['hours_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions9hours_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['minutes_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions11minutes_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['minutes_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions11minutes_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['seconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions11seconds_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['seconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions11seconds_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['microseconds_add'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions10micros_addEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],
    [['microseconds_sub'], 'DATETIME', ['DATETIME', 'INT'],
        '_ZN9starrocks18TimestampFunctions10micros_subEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_6IntValE'],

    [['datediff'], 'INT', ['DATETIME', 'DATETIME'],
        '_ZN9starrocks18TimestampFunctions9date_diffEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValES6_'],
    [['timediff'], 'TIME', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions9time_diffEPN13starrocks_udf'
            '15FunctionContextERKNS1_11DateTimeValES6_'],

    [['date_trunc'], 'DATETIME', ['VARCHAR', 'DATETIME'],
        '_ZN9starrocks18TimestampFunctions14datetime_truncEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_11DateTimeValE',
        '_ZN9starrocks18TimestampFunctions22datetime_trunc_prepareEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks18TimestampFunctions20datetime_trunc_closeEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE'],

    [['date_trunc'], 'DATE', ['VARCHAR', 'DATE'],
        '_ZN9starrocks18TimestampFunctions10date_truncEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_11DateTimeValE',
        '_ZN9starrocks18TimestampFunctions18date_trunc_prepareEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks18TimestampFunctions16date_trunc_closeEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE'],

    [['str_to_date'], 'DATETIME', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks18TimestampFunctions11str_to_dateEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['date_format'], 'VARCHAR', ['DATETIME', 'VARCHAR'],
        '_ZN9starrocks18TimestampFunctions11date_formatEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValE',
        '_ZN9starrocks18TimestampFunctions14format_prepareEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks18TimestampFunctions12format_closeEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE'],
    [['date_format'], 'VARCHAR', ['DATE', 'VARCHAR'],
        '_ZN9starrocks18TimestampFunctions11date_formatEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValE',
        '_ZN9starrocks18TimestampFunctions14format_prepareEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks18TimestampFunctions12format_closeEPN13starrocks_udf'
        '15FunctionContextENS2_18FunctionStateScopeE'],
    [['date', 'to_date'], 'DATE', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions7to_dateEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValE'],

    [['dayname'], 'VARCHAR', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions8day_nameEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],
    [['monthname'], 'VARCHAR', ['DATETIME'],
        '_ZN9starrocks18TimestampFunctions10month_nameEPN13starrocks_udf'
        '15FunctionContextERKNS1_11DateTimeValE'],

    [['convert_tz'], 'DATETIME', ['DATETIME', 'VARCHAR', 'VARCHAR'],
            '_ZN9starrocks18TimestampFunctions10convert_tzEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValERKNS1_9StringValES9_',
            '_ZN9starrocks18TimestampFunctions18convert_tz_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN9starrocks18TimestampFunctions16convert_tz_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],

    [['years_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions10years_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],
    [['months_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions11months_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],
    [['weeks_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions10weeks_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],
    [['days_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions9days_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],
    [['hours_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions10hours_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],
    [['minutes_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions12minutes_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],
    [['seconds_diff'], 'BIGINT', ['DATETIME', 'DATETIME'],
            '_ZN9starrocks18TimestampFunctions12seconds_diffEPN13starrocks_udf15FunctionContextERKNS1_11DateTimeValES6_'],

    # Math builtin functions
    [['pi'], 'DOUBLE', [],
        '_ZN9starrocks13MathFunctions2piEPN13starrocks_udf15FunctionContextE'],
    [['e'], 'DOUBLE', [],
        '_ZN9starrocks13MathFunctions1eEPN13starrocks_udf15FunctionContextE'],

    [['abs'], 'DOUBLE', ['DOUBLE'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['abs'], 'FLOAT', ['FLOAT'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_8FloatValE'],
    [['abs'], 'LARGEINT', ['LARGEINT'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_11LargeIntValE'],
    [['abs'], 'LARGEINT', ['BIGINT'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['abs'], 'INT', ['SMALLINT'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_11SmallIntValE'],
    [['abs'], 'BIGINT', ['INT'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_6IntValE'],
    [['abs'], 'SMALLINT', ['TINYINT'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_10TinyIntValE'],
    [['abs'], 'DECIMALV2', ['DECIMALV2'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_12DecimalV2ValE'],
    [['abs'], 'DECIMAL32', ['DECIMAL32'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_6IntValE'],
    [['abs'], 'DECIMAL64', ['DECIMAL64'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['abs'], 'DECIMAL128', ['DECIMAL128'],
        '_ZN9starrocks13MathFunctions3absEPN13starrocks_udf15FunctionContextERKNS1_11LargeIntValE'],

    [['sign'], 'FLOAT', ['DOUBLE'],
        '_ZN9starrocks13MathFunctions4signEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],

    [['sin'], 'DOUBLE', ['DOUBLE'],
        '_ZN9starrocks13MathFunctions3sinEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['asin'], 'DOUBLE', ['DOUBLE'],
        '_ZN9starrocks13MathFunctions4asinEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['cos'], 'DOUBLE', ['DOUBLE'],
        '_ZN9starrocks13MathFunctions3cosEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['acos'], 'DOUBLE', ['DOUBLE'],
        '_ZN9starrocks13MathFunctions4acosEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['tan'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions3tanEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['atan'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions4atanEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],

    [['ceil', 'ceiling', 'dceil'], 'BIGINT', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions4ceilEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['floor', 'dfloor'], 'BIGINT', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions5floorEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['round', 'dround'], 'BIGINT', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions5roundEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['round', 'dround'], 'DOUBLE', ['DOUBLE', 'INT'],
            '_ZN9starrocks13MathFunctions11round_up_toEPN13starrocks_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE'],
    [['truncate'], 'DOUBLE', ['DOUBLE', 'INT'],
            '_ZN9starrocks13MathFunctions8truncateEPN13starrocks_udf'
            '15FunctionContextERKNS1_9DoubleValERKNS1_6IntValE'],

    [['ln', 'dlog1'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions2lnEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['log'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
            '_ZN9starrocks13MathFunctions3logEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValES6_'],
    [['log2'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions4log2EPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['log10', 'dlog10'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions5log10EPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['exp', 'dexp'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions3expEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],

    [['radians'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions7radiansEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['degrees'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions7degreesEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],

    [['sqrt', 'dsqrt'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions4sqrtEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['pow', 'power', 'dpow', 'fpow'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
            '_ZN9starrocks13MathFunctions3powEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValES6_'],

    [['rand', 'random'], 'DOUBLE', [],
            '_ZN9starrocks13MathFunctions4randEPN13starrocks_udf15FunctionContextE',
            '_ZN9starrocks13MathFunctions12rand_prepareEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['rand', 'random'], 'DOUBLE', ['BIGINT'],
            '_ZN9starrocks13MathFunctions9rand_seedEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE',
            '_ZN9starrocks13MathFunctions12rand_prepareEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],

    [['bin'], 'VARCHAR', ['BIGINT'],
            '_ZN9starrocks13MathFunctions3binEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['hex'], 'VARCHAR', ['BIGINT'],
            '_ZN9starrocks13MathFunctions7hex_intEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['hex'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks13MathFunctions10hex_stringEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['unhex'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks13MathFunctions5unhexEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],

    [['conv'], 'VARCHAR', ['BIGINT', 'TINYINT', 'TINYINT'],
            '_ZN9starrocks13MathFunctions8conv_intEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValERKNS1_10TinyIntValES9_'],
    [['conv'], 'VARCHAR', ['VARCHAR', 'TINYINT', 'TINYINT'],
            '_ZN9starrocks13MathFunctions11conv_stringEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_10TinyIntValES9_'],

    [['pmod'], 'BIGINT', ['BIGINT', 'BIGINT'],
            '_ZN9starrocks13MathFunctions11pmod_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['pmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
            '_ZN9starrocks13MathFunctions11pmod_doubleEPN13starrocks_udf'
            '15FunctionContextERKNS1_9DoubleValES6_'],
    [['mod'], 'TINYINT', ['TINYINT', 'TINYINT'],
            '_ZN9starrocks9Operators29mod_tiny_int_val_tiny_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_10TinyIntValES6_'],
    [['mod'], 'SMALLINT', ['SMALLINT', 'SMALLINT'],
            '_ZN9starrocks9Operators31mod_small_int_val_small_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_11SmallIntValES6_'],
    [['mod'], 'INT', ['INT', 'INT'],
            '_ZN9starrocks9Operators19mod_int_val_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_6IntValES6_'],
    [['mod'], 'BIGINT', ['BIGINT', 'BIGINT'],
            '_ZN9starrocks9Operators27mod_big_int_val_big_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['mod'], 'LARGEINT', ['LARGEINT', 'LARGEINT'],
            '_ZN9starrocks9Operators31mod_large_int_val_large_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_11LargeIntValES6_'],
    [['mod'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'],
            '_ZN9starrocks18DecimalV2Operators31mod_decimalv2_val_decimalv2_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_12DecimalV2ValES6_'],
    [['mod', 'fmod'], 'DOUBLE', ['DOUBLE', 'DOUBLE'],
        '_ZN9starrocks13MathFunctions11fmod_doubleEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValES6_'],
    [['mod', 'fmod'], 'FLOAT', ['FLOAT', 'FLOAT'],
        '_ZN9starrocks13MathFunctions10fmod_floatEPN13starrocks_udf15FunctionContextERKNS1_8FloatValES6_'],
    [['mod'], 'DECIMAL32', ['DECIMAL32', 'DECIMAL32'],
            '_ZN9starrocks9Operators19mod_int_val_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_6IntValES6_'],
    [['mod'], 'DECIMAL64', ['DECIMAL64', 'DECIMAL64'],
            '_ZN9starrocks9Operators27mod_big_int_val_big_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValES6_'],
    [['mod'], 'DECIMAL128', ['DECIMAL128', 'DECIMAL128'],
            '_ZN9starrocks9Operators31mod_large_int_val_large_int_valEPN13starrocks_udf'
            '15FunctionContextERKNS1_11LargeIntValES6_'],

    [['positive'], 'BIGINT', ['BIGINT'],
            '_ZN9starrocks13MathFunctions15positive_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['positive'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions15positive_doubleEPN13starrocks_udf'
            '15FunctionContextERKNS1_9DoubleValE'],
    [['positive'], 'DECIMALV2', ['DECIMALV2'],
            '_ZN9starrocks13MathFunctions16positive_decimalEPN13starrocks_udf'
            '15FunctionContextERKNS1_12DecimalV2ValE'],
    [['positive'], 'DECIMAL32', ['DECIMAL32'],
            '_ZN9starrocks13MathFunctions15positive_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['positive'], 'DECIMAL64', ['DECIMAL64'],
            '_ZN9starrocks13MathFunctions15positive_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['positive'], 'DECIMAL128', ['DECIMAL128'],
            '_ZN9starrocks13MathFunctions15positive_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],

    [['negative'], 'BIGINT', ['BIGINT'],
            '_ZN9starrocks13MathFunctions15negative_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['negative'], 'DOUBLE', ['DOUBLE'],
            '_ZN9starrocks13MathFunctions15negative_doubleEPN13starrocks_udf'
            '15FunctionContextERKNS1_9DoubleValE'],
    [['negative'], 'DECIMALV2', ['DECIMALV2'],
            '_ZN9starrocks13MathFunctions16negative_decimalEPN13starrocks_udf'
            '15FunctionContextERKNS1_12DecimalV2ValE'],
    [['negative'], 'DECIMAL32', ['DECIMAL32'],
            '_ZN9starrocks13MathFunctions15negative_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['negative'], 'DECIMAL64', ['DECIMAL64'],
            '_ZN9starrocks13MathFunctions15negative_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],
    [['negative'], 'DECIMAL128', ['DECIMAL128'],
            '_ZN9starrocks13MathFunctions15negative_bigintEPN13starrocks_udf'
            '15FunctionContextERKNS1_9BigIntValE'],

    [['least'], 'TINYINT', ['TINYINT', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_10TinyIntValE'],
    [['least'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_11SmallIntValE'],
    [['least'], 'INT', ['INT', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_6IntValE'],
    [['least'], 'BIGINT', ['BIGINT', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['least'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['least'], 'DECIMAL32', ['DECIMAL32', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_6IntValE'],
    [['least'], 'DECIMAL64', ['DECIMAL64', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['least'], 'DECIMAL128', ['DECIMAL128', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['least'], 'FLOAT', ['FLOAT', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_8FloatValE'],
    [['least'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_9DoubleValE'],
    [['least'], 'DATETIME', ['DATETIME', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_11DateTimeValE'],
    [['least'], 'DECIMALV2', ['DECIMALV2', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_12DecimalV2ValE'],
    [['least'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN9starrocks13MathFunctions5leastEPN13starrocks_udf15FunctionContextEiPKNS1_9StringValE'],

    [['greatest'], 'TINYINT', ['TINYINT', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_10TinyIntValE'],
    [['greatest'], 'SMALLINT', ['SMALLINT', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_11SmallIntValE'],
    [['greatest'], 'INT', ['INT', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_6IntValE'],
    [['greatest'], 'BIGINT', ['BIGINT', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['greatest'], 'LARGEINT', ['LARGEINT', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['greatest'], 'DECIMAL32', ['DECIMAL32', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_6IntValE'],
    [['greatest'], 'DECIMAL64', ['DECIMAL64', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_9BigIntValE'],
    [['greatest'], 'DECIMAL128', ['DECIMAL128', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_11LargeIntValE'],
    [['greatest'], 'FLOAT', ['FLOAT', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_8FloatValE'],
    [['greatest'], 'DOUBLE', ['DOUBLE', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_9DoubleValE'],
    [['greatest'], 'DECIMALV2', ['DECIMALV2', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_12DecimalV2ValE'],
    [['greatest'], 'DATETIME', ['DATETIME', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_11DateTimeValE'],
    [['greatest'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN9starrocks13MathFunctions8greatestEPN13starrocks_udf15FunctionContextEiPKNS1_9StringValE'],

    # Conditional Functions
    # Some of these have empty symbols because the BE special-cases them based on the
    # function name
    [['if'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN', 'BOOLEAN'], ''],
    [['if'], 'TINYINT', ['BOOLEAN', 'TINYINT', 'TINYINT'], ''],
    [['if'], 'SMALLINT', ['BOOLEAN', 'SMALLINT', 'SMALLINT'], ''],
    [['if'], 'INT', ['BOOLEAN', 'INT', 'INT'], ''],
    [['if'], 'BIGINT', ['BOOLEAN', 'BIGINT', 'BIGINT'], ''],
    [['if'], 'LARGEINT', ['BOOLEAN', 'LARGEINT', 'LARGEINT'], ''],
    [['if'], 'FLOAT', ['BOOLEAN', 'FLOAT', 'FLOAT'], ''],
    [['if'], 'DOUBLE', ['BOOLEAN', 'DOUBLE', 'DOUBLE'], ''],
    [['if'], 'DATETIME', ['BOOLEAN', 'DATETIME', 'DATETIME'], ''],
    [['if'], 'DATE', ['BOOLEAN', 'DATE', 'DATE'], ''],
    [['if'], 'DECIMALV2', ['BOOLEAN', 'DECIMALV2', 'DECIMALV2'], ''],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['if'], 'VARCHAR', ['BOOLEAN', 'VARCHAR', 'VARCHAR'], ''],
    [['if'], 'BITMAP', ['BOOLEAN', 'BITMAP', 'BITMAP'], ''],
    [['if'], 'PERCENTILE', ['BOOLEAN', 'PERCENTILE', 'PERCENTILE'], ''],
    [['if'], 'HLL', ['BOOLEAN', 'HLL', 'HLL'], ''],
    [['if'], 'DECIMAL32', ['BOOLEAN', 'DECIMAL32', 'DECIMAL32'], ''],
    [['if'], 'DECIMAL64', ['BOOLEAN', 'DECIMAL64', 'DECIMAL64'], ''],
    [['if'], 'DECIMAL128', ['BOOLEAN', 'DECIMAL128', 'DECIMAL128'], ''],

    [['nullif'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], ''],
    [['nullif'], 'TINYINT', ['TINYINT', 'TINYINT'], ''],
    [['nullif'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], ''],
    [['nullif'], 'INT', ['INT', 'INT'], ''],
    [['nullif'], 'BIGINT', ['BIGINT', 'BIGINT'], ''],
    [['nullif'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], ''],
    [['nullif'], 'FLOAT', ['FLOAT', 'FLOAT'], ''],
    [['nullif'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], ''],
    [['nullif'], 'DATETIME', ['DATETIME', 'DATETIME'], ''],
    [['nullif'], 'DATE', ['DATE', 'DATE'], ''],
    [['nullif'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], ''],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['nullif'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], ''],
    [['nullif'], 'BITMAP', ['BITMAP', 'BITMAP'], ''],
    [['nullif'], 'PERCENTILE', ['PERCENTILE', 'PERCENTILE'], ''],
    [['nullif'], 'HLL', ['HLL', 'HLL'], ''],
    [['nullif'], 'DECIMAL32', ['DECIMAL32', 'DECIMAL32'], ''],
    [['nullif'], 'DECIMAL64', ['DECIMAL64', 'DECIMAL64'], ''],
    [['nullif'], 'DECIMAL128', ['DECIMAL128', 'DECIMAL128'], ''],

    [['ifnull'], 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], ''],
    [['ifnull'], 'TINYINT', ['TINYINT', 'TINYINT'], ''],
    [['ifnull'], 'SMALLINT', ['SMALLINT', 'SMALLINT'], ''],
    [['ifnull'], 'INT', ['INT', 'INT'], ''],
    [['ifnull'], 'BIGINT', ['BIGINT', 'BIGINT'], ''],
    [['ifnull'], 'LARGEINT', ['LARGEINT', 'LARGEINT'], ''],
    [['ifnull'], 'FLOAT', ['FLOAT', 'FLOAT'], ''],
    [['ifnull'], 'DOUBLE', ['DOUBLE', 'DOUBLE'], ''],
    [['ifnull'], 'DATE', ['DATE', 'DATE'], ''],
    [['ifnull'], 'DATETIME', ['DATETIME', 'DATETIME'], ''],
    [['ifnull'], 'DECIMALV2', ['DECIMALV2', 'DECIMALV2'], ''],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['ifnull'], 'VARCHAR', ['VARCHAR', 'VARCHAR'], ''],
    [['ifnull'], 'BITMAP', ['BITMAP', 'BITMAP'], ''],
    [['ifnull'], 'PERCENTILE', ['PERCENTILE', 'PERCENTILE'], ''],
    [['ifnull'], 'HLL', ['HLL', 'HLL'], ''],
    [['ifnull'], 'DECIMAL32', ['DECIMAL32', 'DECIMAL32'], ''],
    [['ifnull'], 'DECIMAL64', ['DECIMAL64', 'DECIMAL64'], ''],
    [['ifnull'], 'DECIMAL128', ['DECIMAL128', 'DECIMAL128'], ''],

    [['coalesce'], 'BOOLEAN', ['BOOLEAN', '...'], ''],
    [['coalesce'], 'TINYINT', ['TINYINT', '...'], ''],
    [['coalesce'], 'SMALLINT', ['SMALLINT', '...'], ''],
    [['coalesce'], 'INT', ['INT', '...'], ''],
    [['coalesce'], 'BIGINT', ['BIGINT', '...'], ''],
    [['coalesce'], 'LARGEINT', ['LARGEINT', '...'], ''],
    [['coalesce'], 'FLOAT', ['FLOAT', '...'], ''],
    [['coalesce'], 'DOUBLE', ['DOUBLE', '...'], ''],
    [['coalesce'], 'DATETIME', ['DATETIME', '...'], ''],
    [['coalesce'], 'DATE', ['DATE', '...'], ''],
    [['coalesce'], 'DECIMALV2', ['DECIMALV2', '...'], ''],
    # The priority of varchar should be lower than decimal in IS_SUPERTYPE_OF mode.
    [['coalesce'], 'VARCHAR', ['VARCHAR', '...'], ''],
    [['coalesce'], 'BITMAP', ['BITMAP', '...'], ''],
    [['coalesce'], 'PERCENTILE', ['PERCENTILE', '...'], ''],
    [['coalesce'], 'HLL', ['HLL', '...'], ''],
    [['coalesce'], 'DECIMAL32', ['DECIMAL32', '...'], ''],
    [['coalesce'], 'DECIMAL64', ['DECIMAL64', '...'], ''],
    [['coalesce'], 'DECIMAL128', ['DECIMAL128', '...'], ''],

    [['esquery'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks11ESFunctions5matchEPN'
        '9starrocks_udf15FunctionContextERKNS1_9StringValES6_'],

    # String builtin functions
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN9starrocks15StringFunctions9substringEPN'
        '9starrocks_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['substr', 'substring'], 'VARCHAR', ['VARCHAR', 'INT', 'INT'],
        '_ZN9starrocks15StringFunctions9substringEPN'
        '9starrocks_udf15FunctionContextERKNS1_9StringValERKNS1_6IntValES9_'],
    [['strleft', 'left'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN9starrocks15StringFunctions4leftEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['strright', 'right'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN9starrocks15StringFunctions5rightEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['ends_with'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks15StringFunctions9ends_withEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['starts_with'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks15StringFunctions11starts_withEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['null_or_empty'], 'BOOLEAN', ['VARCHAR'],
        '_ZN9starrocks15StringFunctions13null_or_emptyEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['space'], 'VARCHAR', ['INT'],
        '_ZN9starrocks15StringFunctions5spaceEPN13starrocks_udf15FunctionContextERKNS1_6IntValE'],
    [['repeat'], 'VARCHAR', ['VARCHAR', 'INT'],
        '_ZN9starrocks15StringFunctions6repeatEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValERKNS1_6IntValE'],
    [['lpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions4lpadEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_'],
    [['rpad'], 'VARCHAR', ['VARCHAR', 'INT', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions4rpadEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValERKNS1_6IntValES6_'],
    [['append_trailing_char_if_absent'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
	'_ZN9starrocks15StringFunctions30append_trailing_char_if_absentEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['length'], 'INT', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions6lengthEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['char_length', 'character_length'], 'INT', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions16char_utf8_lengthEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
   [['lower', 'lcase'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions5lowerEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['upper', 'ucase'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions5upperEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['reverse'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions7reverseEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['trim'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions4trimEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['ltrim'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions5ltrimEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['rtrim'], 'VARCHAR', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions5rtrimEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['ascii'], 'INT', ['VARCHAR'],
            '_ZN9starrocks15StringFunctions5asciiEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['instr'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions5instrEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions6locateEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['locate'], 'INT', ['VARCHAR', 'VARCHAR', 'INT'],
            '_ZN9starrocks15StringFunctions10locate_posEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_6IntValE'],
    [['regexp_extract'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'BIGINT'],
            '_ZN9starrocks15StringFunctions14regexp_extractEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValES6_RKNS1_9BigIntValE',
            '_ZN9starrocks15StringFunctions14regexp_prepareEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN9starrocks15StringFunctions12regexp_closeEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['regexp_replace'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions14regexp_replaceEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN9starrocks15StringFunctions14regexp_prepareEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN9starrocks15StringFunctions12regexp_closeEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['concat'], 'VARCHAR', ['VARCHAR', '...'],
            '_ZN9starrocks15StringFunctions6concatEPN13starrocks_udf15FunctionContextEiPKNS1_9StringValE'],
    [['concat_ws'], 'VARCHAR', ['VARCHAR', 'VARCHAR', '...'],
            '_ZN9starrocks15StringFunctions9concat_wsEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValEiPS5_'],
    [['find_in_set'], 'INT', ['VARCHAR', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions11find_in_setEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValES6_'],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions9parse_urlEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValES6_',
            '_ZN9starrocks15StringFunctions17parse_url_prepareEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN9starrocks15StringFunctions15parse_url_closeEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['parse_url'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'VARCHAR'],
            '_ZN9starrocks15StringFunctions13parse_url_keyEPN13starrocks_udf'
            '15FunctionContextERKNS1_9StringValES6_S6_',
            '_ZN9starrocks15StringFunctions17parse_url_prepareEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE',
            '_ZN9starrocks15StringFunctions15parse_url_closeEPN13starrocks_udf'
            '15FunctionContextENS2_18FunctionStateScopeE'],
    [['money_format'], 'VARCHAR', ['BIGINT'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['money_format'], 'VARCHAR', ['LARGEINT'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_11LargeIntValE'],
    [['money_format'], 'VARCHAR', ['DOUBLE'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
    [['money_format'], 'VARCHAR', ['DECIMALV2'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_12DecimalV2ValE'],
     [['split_part'], 'VARCHAR', ['VARCHAR', 'VARCHAR', 'INT'],
        '_ZN9starrocks15StringFunctions10split_partEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_RKNS1_6IntValE'],
    [['money_format'], 'VARCHAR', ['DECIMAL32'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['money_format'], 'VARCHAR', ['DECIMAL64'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['money_format'], 'VARCHAR', ['DECIMAL128'],
        '_ZN9starrocks15StringFunctions12money_formatEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],

    # Utility functions
    [['sleep'], 'BOOLEAN', ['INT'],
        '_ZN9starrocks16UtilityFunctions5sleepEPN13starrocks_udf15FunctionContextERKNS1_6IntValE'],
    [['version'], 'VARCHAR', [],
        '_ZN9starrocks16UtilityFunctions7versionEPN13starrocks_udf15FunctionContextE'],
    [['current_version'], 'VARCHAR', [],
        '_ZN9starrocks16UtilityFunctions15current_versionEPN13starrocks_udf15FunctionContextE'],
    [['last_query_id'], 'VARCHAR', [],
        '_ZN9starrocks16UtilityFunctions13last_query_idEPN13starrocks_udf15FunctionContextE'],

    # Json functions
    [['get_json_int'], 'INT', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks13JsonFunctions12get_json_intEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN9starrocks13JsonFunctions17json_path_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks13JsonFunctions15json_path_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],
    [['get_json_double'], 'DOUBLE', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks13JsonFunctions15get_json_doubleEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN9starrocks13JsonFunctions17json_path_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks13JsonFunctions15json_path_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],
    [['get_json_string'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks13JsonFunctions15get_json_stringEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN9starrocks13JsonFunctions17json_path_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks13JsonFunctions15json_path_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],

    #hll function
    [['hll_cardinality'], 'BIGINT', ['HLL'],
     '_ZN9starrocks12HllFunctions15hll_cardinalityEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['hll_cardinality'], 'BIGINT', ['VARCHAR'],
        '_ZN9starrocks12HllFunctions15hll_cardinalityEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['hll_hash'], 'HLL', ['VARCHAR'],
        '_ZN9starrocks12HllFunctions8hll_hashEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['hll_empty'], 'HLL', [],
        '_ZN9starrocks12HllFunctions9hll_emptyEPN13starrocks_udf15FunctionContextE'],

    #bitmap function

    [['to_bitmap'], 'BITMAP', ['VARCHAR'],
        '_ZN9starrocks15BitmapFunctions9to_bitmapEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['bitmap_hash'], 'BITMAP', ['VARCHAR'],
        '_ZN9starrocks15BitmapFunctions11bitmap_hashEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['bitmap_count'], 'BIGINT', ['BITMAP'],
        '_ZN9starrocks15BitmapFunctions12bitmap_countEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['bitmap_empty'], 'BITMAP', [],
        '_ZN9starrocks15BitmapFunctions12bitmap_emptyEPN13starrocks_udf15FunctionContextE'],
    [['bitmap_or'], 'BITMAP', ['BITMAP','BITMAP'],
        '_ZN9starrocks15BitmapFunctions9bitmap_orEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['bitmap_and'], 'BITMAP', ['BITMAP','BITMAP'],
        '_ZN9starrocks15BitmapFunctions10bitmap_andEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],
    [['bitmap_to_string'], 'VARCHAR', ['BITMAP'],
        '_ZN9starrocks15BitmapFunctions16bitmap_to_stringEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['bitmap_from_string'], 'BITMAP', ['VARCHAR'],
        '_ZN9starrocks15BitmapFunctions18bitmap_from_stringEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['bitmap_contains'], 'BOOLEAN', ['BITMAP','BIGINT'],
        '_ZN9starrocks15BitmapFunctions15bitmap_containsEPN13starrocks_udf15FunctionContextERKNS1_9StringValERKNS1_9BigIntValE'],
    [['bitmap_has_any'], 'BOOLEAN', ['BITMAP','BITMAP'],
        '_ZN9starrocks15BitmapFunctions14bitmap_has_anyEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_'],

   #percentile function
   [['percentile_hash'], 'PERCENTILE', ['DOUBLE'],
        '_ZN9starrocks19PercentileFunctions15percentile_hashEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValE'],
   [['percentile_empty'], 'PERCENTILE', [],
        '_ZN9starrocks19PercentileFunctions16percentile_emptyEPN13starrocks_udf15FunctionContextE'],
   [['percentile_approx_raw'], 'DOUBLE', ['PERCENTILE', 'DOUBLE'],
        '_ZN9starrocks19PercentileFunctions21percentile_approx_rawEPN13starrocks_udf15FunctionContextERKNS1_9StringValERKNS1_9DoubleValE'],

    # hash functions
    [['murmur_hash3_32'], 'INT', ['VARCHAR', '...'],
        '_ZN9starrocks13HashFunctions15murmur_hash3_32EPN13starrocks_udf15FunctionContextEiPKNS1_9StringValE'],

    # aes and base64 function
    [['aes_encrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks19EncryptionFunctions11aes_encryptEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['aes_decrypt'], 'VARCHAR', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks19EncryptionFunctions11aes_decryptEPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValES6_'],
    [['from_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks19EncryptionFunctions11from_base64EPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValE'],
    [['to_base64'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks19EncryptionFunctions9to_base64EPN13starrocks_udf'
        '15FunctionContextERKNS1_9StringValE'],
    # for compatable with MySQL
    [['md5'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks19EncryptionFunctions3md5EPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['md5sum'], 'VARCHAR', ['VARCHAR', '...'],
        '_ZN9starrocks19EncryptionFunctions6md5sumEPN13starrocks_udf15FunctionContextEiPKNS1_9StringValE'],

    # geo functions
    [['ST_Point'], 'VARCHAR', ['DOUBLE', 'DOUBLE'],
        '_ZN9starrocks12GeoFunctions8st_pointEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValES6_'],
    [['ST_X'], 'DOUBLE', ['VARCHAR'],
        '_ZN9starrocks12GeoFunctions4st_xEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['ST_Y'], 'DOUBLE', ['VARCHAR'],
        '_ZN9starrocks12GeoFunctions4st_yEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],

    [['ST_Distance_Sphere'], 'DOUBLE', ['DOUBLE', 'DOUBLE', 'DOUBLE', 'DOUBLE'],
        '_ZN9starrocks12GeoFunctions18st_distance_sphereEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValES6_S6_S6_'],

    [['ST_AsText', 'ST_AsWKT'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks12GeoFunctions9st_as_wktEPN13starrocks_udf15FunctionContextERKNS1_9StringValE'],
    [['ST_GeometryFromText', 'ST_GeomFromText'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks12GeoFunctions11st_from_wktEPN13starrocks_udf15FunctionContextERKNS1_9StringValE',
        '_ZN9starrocks12GeoFunctions19st_from_wkt_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks12GeoFunctions17st_from_wkt_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],

    [['ST_LineFromText', 'ST_LineStringFromText'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks12GeoFunctions7st_lineEPN13starrocks_udf15FunctionContextERKNS1_9StringValE',
        '_ZN9starrocks12GeoFunctions15st_line_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks12GeoFunctions17st_from_wkt_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],

    [['ST_Polygon', 'ST_PolyFromText', 'ST_PolygonFromText'], 'VARCHAR', ['VARCHAR'],
        '_ZN9starrocks12GeoFunctions10st_polygonEPN13starrocks_udf15FunctionContextERKNS1_9StringValE',
        '_ZN9starrocks12GeoFunctions18st_polygon_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks12GeoFunctions17st_from_wkt_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],

    [['ST_Circle'], 'VARCHAR', ['DOUBLE', 'DOUBLE', 'DOUBLE'],
        '_ZN9starrocks12GeoFunctions9st_circleEPN13starrocks_udf15FunctionContextERKNS1_9DoubleValES6_S6_',
        '_ZN9starrocks12GeoFunctions17st_circle_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks12GeoFunctions17st_from_wkt_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],

    [['ST_Contains'], 'BOOLEAN', ['VARCHAR', 'VARCHAR'],
        '_ZN9starrocks12GeoFunctions11st_containsEPN13starrocks_udf15FunctionContextERKNS1_9StringValES6_',
        '_ZN9starrocks12GeoFunctions19st_contains_prepareEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE',
        '_ZN9starrocks12GeoFunctions17st_contains_closeEPN13starrocks_udf15FunctionContextENS2_18FunctionStateScopeE'],
    # grouping sets functions
    [['grouping_id'], 'BIGINT', ['BIGINT'],
        '_ZN9starrocks21GroupingSetsFunctions11grouping_idEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],
    [['grouping'], 'BIGINT', ['BIGINT'], '_ZN9starrocks21GroupingSetsFunctions8groupingEPN13starrocks_udf15FunctionContextERKNS1_9BigIntValE'],

    [['array_length'], 'INT', ['ANY_ARRAY'], ''],
    [['array_append'], 'ANY_ARRAY', ['ANY_ARRAY', 'ANY_ELEMENT'], ''],
    [['array_contains'], 'BOOLEAN', ['ANY_ARRAY', 'ANY_ELEMENT'], ''],
]

# Except the following functions, other function will directly return
# null if there is null parameters.
# Functions in this set will handle null values, not just return null.
#
# This set is only used to replace 'functions with null parameters' with NullLiteral
# when applying FoldConstantsRule rules on the FE side.
# TODO(cmy): Are these functions only required to handle null values?
non_null_result_with_null_param_functions = [
    'if',
    'hll_hash',
    'concat_ws',
    'ifnull',
    'nullif',
    'null_or_empty',
    'coalesce'
]

invisible_functions = [
]
