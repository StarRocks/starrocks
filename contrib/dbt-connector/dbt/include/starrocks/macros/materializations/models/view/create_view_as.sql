/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
  create view {{ relation }} as {{ sql }};
{%- endmacro %}
