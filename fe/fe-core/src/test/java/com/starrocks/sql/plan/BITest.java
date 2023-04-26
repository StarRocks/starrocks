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


package com.starrocks.sql.plan;

import org.junit.Test;

public class BITest extends PlanTestBase {
    @Test
    public void testMetabaseQueryColumn() throws Exception {
        String sql = "SELECT TABLE_SCHEMA TABLE_CAT, NULL TABLE_SCHEM, TABLE_NAME, COLUMN_NAME, " +
                "CASE data_type WHEN 'bit' THEN -7 " +
                "WHEN 'tinyblob' THEN -3 " +
                "WHEN 'mediumblob' THEN -4 " +
                "WHEN 'longblob' THEN -4 " +
                "WHEN 'blob' THEN -4 " +
                "WHEN 'tinytext' THEN 12 " +
                "WHEN 'mediumtext' THEN -1 " +
                "WHEN 'longtext' THEN -1 " +
                "WHEN 'text' THEN -1 " +
                "WHEN 'date' THEN 91 " +
                "WHEN 'datetime' THEN 93 " +
                "WHEN 'decimal' THEN 3 " +
                "WHEN 'double' THEN 8 " +
                "WHEN 'enum' THEN 12 " +
                "WHEN 'float' THEN 7 " +
                "WHEN 'int' THEN IF( COLUMN_TYPE like '%unsigned%', 4,4) " +
                "WHEN 'bigint' THEN -5 " +
                "WHEN 'mediumint' THEN 4 " +
                "WHEN 'null' THEN 0 WHEN 'set' THEN 12 " +
                "WHEN 'smallint' THEN IF( COLUMN_TYPE like '%unsigned%', 5,5) " +
                "WHEN 'varchar' THEN 12 WHEN 'varbinary' THEN -3 " +
                "WHEN 'char' THEN 1 WHEN 'binary' THEN -2 " +
                "WHEN 'time' THEN 92 WHEN 'timestamp' THEN 93 " +
                "WHEN 'tinyint' THEN IF(COLUMN_TYPE like 'tinyint(1)%',-7,-6)  " +
                "WHEN 'year' THEN 91 ELSE 1111 END  DATA_TYPE, " +
                "IF(COLUMN_TYPE like 'tinyint(1)%', 'BIT',  " +
                "UCASE(IF( COLUMN_TYPE LIKE '%(%)%', CONCAT(SUBSTRING( COLUMN_TYPE,1, LOCATE('(',COLUMN_TYPE) - 1 ), " +
                "SUBSTRING(COLUMN_TYPE ,1+locate(')', COLUMN_TYPE))), COLUMN_TYPE))) TYPE_NAME,  " +
                "CASE DATA_TYPE  WHEN 'time' THEN IF(DATETIME_PRECISION = 0, 10, " +
                "CAST(11 + DATETIME_PRECISION as signed integer))  WHEN 'date' THEN 10  " +
                "WHEN 'datetime' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  " +
                "WHEN 'timestamp' THEN IF(DATETIME_PRECISION = 0, 19, CAST(20 + DATETIME_PRECISION as signed integer))  " +
                "ELSE   IF(NUMERIC_PRECISION IS NULL, LEAST(CHARACTER_MAXIMUM_LENGTH,2147483647), NUMERIC_PRECISION)  " +
                "END COLUMN_SIZE, 65535 BUFFER_LENGTH,  CONVERT (CASE DATA_TYPE " +
                "WHEN 'year' THEN NUMERIC_SCALE " +
                "WHEN 'tinyint' THEN 0 ELSE NUMERIC_SCALE END, UNSIGNED INTEGER) DECIMAL_DIGITS, 10 NUM_PREC_RADIX, " +
                "IF(IS_NULLABLE = 'yes',1,0) NULLABLE,COLUMN_COMMENT REMARKS, " +
                "COLUMN_DEFAULT COLUMN_DEF, 0 SQL_DATA_TYPE, 0 SQL_DATETIME_SUB,   " +
                "LEAST(CHARACTER_OCTET_LENGTH,2147483647) CHAR_OCTET_LENGTH, " +
                "ORDINAL_POSITION, IS_NULLABLE, NULL SCOPE_CATALOG, NULL SCOPE_SCHEMA, " +
                "NULL SCOPE_TABLE, NULL SOURCE_DATA_TYPE, IF(EXTRA = 'auto_increment','YES','NO') IS_AUTOINCREMENT,  " +
                "IF(EXTRA in ('VIRTUAL', 'PERSISTENT', 'VIRTUAL GENERATED', 'STORED GENERATED') ," +
                "'YES','NO') IS_GENERATEDCOLUMN  FROM INFORMATION_SCHEMA.COLUMNS  " +
                "WHERE TABLE_SCHEMA = database() AND TABLE_NAME='base1' " +
                "ORDER BY TABLE_CAT, TABLE_SCHEM, TABLE_NAME, ORDINAL_POSITION";

        getFragmentPlan(sql);
    }
}
