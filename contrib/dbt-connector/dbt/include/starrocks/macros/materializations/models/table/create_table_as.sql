/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__create_table_as(temporary, relation, sql) -%}
  {% set sql_header = config.get('sql_header', none) %}
  {% set engine = config.get('engine', 'OLAP') %}

  {{ sql_header if sql_header is not none }}

  create table {{ relation.include(database=False) }}
  {% if engine == 'OLAP' %}
    {{ starrocks__olap_table(True) }}
  {% else %}
    {% set msg -%}
      "ENGINE = {{ engine }}" is not "CREATE TABLE ... AS ..."
    {%- endset %}
    {{ exceptions.raise_compiler_error(msg) }}
  {% endif %}

  as {{ sql }}

{%- endmacro %}
