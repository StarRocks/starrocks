/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__get_columns_in_relation(relation) -%}
  {% call statement('get_columns_in_relation', fetch_result=True) %}
    select
        column_name,
        data_type,
        character_maximum_length,
        numeric_precision,
        numeric_scale

    from INFORMATION_SCHEMA.columns
    where table_name = '{{ relation.identifier }}'
      {% if relation.schema %}
      and table_schema = '{{ relation.schema }}'
      {% endif %}
    order by ordinal_position

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}