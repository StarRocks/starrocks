/*
 * Copyright 2021-present StarRocks, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https:*www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

{% macro starrocks__create_table_as(temporary, relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set engine = config.get('engine', 'OLAP') -%}

  {{ sql_header if sql_header is not none }}

  create table {{ relation.include(database=False) }}
  {%- if engine == 'OLAP' -%}
    {{ starrocks__olap_table(True) }}
  {%- else -%}
    {%- set msg -%}
      "ENGINE = {{ engine }}" does not support, currently only supports 'OLAP'
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg) }}
  {%- endif -%}

  as {{ sql }}

{%- endmacro %}
