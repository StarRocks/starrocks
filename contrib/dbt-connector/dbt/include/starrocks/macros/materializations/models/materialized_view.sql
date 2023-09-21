{% macro starrocks__get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) %}
    {{ starrocks__get_drop_relation_sql(existing_relation) }}
    {{ get_create_materialized_view_as_sql(relation, sql) }}
{% endmacro %}

{% macro starrocks__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }};
{%- endmacro %}

{% macro starrocks__get_create_materialized_view_as_sql(relation, sql) %}

    {%- set partition_by = config.get('partition_by') -%}
    {%- set buckets = config.get('buckets') -%}
    {%- set distributed_by = config.get('distributed_by') -%}
    {%- set properties = config.get('properties') -%}
    {%- set refresh_method = config.get('refresh_method', 'manual') -%}

    create materialized view {{ relation }}

    {%- if partition_by is not none -%}
        PARTITION BY (
        {%- for col in partition_by -%}
         {{ col }} {%- if not loop.last -%}, {%- endif -%}
        {%- endfor -%}
        )
    {%- endif -%}

    {%- if distributed_by is not none %}
    DISTRIBUTED BY HASH (
      {%- for item in distributed_by -%}
        {{ item }} {%- if not loop.last -%}, {%- endif -%}
      {%- endfor -%} )
      {%- if buckets is not none %}
        BUCKETS {{ buckets }}
      {% endif -%}
    {%- elif adapter.is_before_version("3.1.0") -%}
      {%- set msg -%}
        [distributed_by] must set before version 3.1, current version is {{ adapter.current_version() }}
      {%- endset -%}
      {{ exceptions.raise_compiler_error(msg) }}
    {% endif -%}
    refresh {{ refresh_method }}
    {% if properties is not none %}
    PROPERTIES (
      {% for key, value in properties.items() %}
        "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
      {% endfor %}
    )
    {% endif %}
    as
    {{ sql }};

{% endmacro %}

{% macro starrocks__get_drop_relation_sql(relation) %}
    {% call statement(name="main") %}
        {%- if relation.is_materialized_view -%}
            {{ starrocks__drop_materialized_view(relation) }}
        {%- else -%}
            drop {{ relation.type }} if exists {{ relation }};
        {%- endif -%}
    {% endcall %}
{% endmacro %}

{% macro starrocks__get_materialized_view_configuration_changes(existing_relation, new_config) %}
{% endmacro %}

{% macro starrocks__get_alter_materialized_view_as_sql(
    relation,
    configuration_changes,
    sql,
    existing_relation,
    backup_relation,
    intermediate_relation
) %}

    {{ get_replace_materialized_view_as_sql(relation, sql, existing_relation, backup_relation, intermediate_relation) }}

{% endmacro %}