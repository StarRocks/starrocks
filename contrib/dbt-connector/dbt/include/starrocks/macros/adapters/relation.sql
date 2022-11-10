/*
 * This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
 */

{% macro starrocks__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) %}
    drop {{ relation.type }} if exists {{ relation }}
  {% endcall %}
{%- endmacro %}

{% macro starrocks__rename_relation(from_relation, to_relation) -%}
  {% call statement('rename_relation') %}
    {% if to_relation.is_view %}
      {% set results = run_query('show create view ' + from_relation.render() ) %}
        create view {{ to_relation }} as {{ results[0]['Create View'].replace(from_relation.table, to_relation.table).split('AS',1)[1] }}
        {% call statement('drop_view') %}
          drop view if exists {{ from_relation }}
        {% endcall %}
      {% else %}
      alter table {{ from_relation }} rename {{ to_relation.table }}
    {% endif %}
  {% endcall %}
{%- endmacro %}
