{#
    Project-level override of the dbt-starrocks adapter's `table`
    materialization (dbt/include/starrocks/macros/materializations/table.sql),
    with one behavioral fix and one guard:

    FIX: the adapter's on_table_exists='ignore' branch returns without ever
    defining the `main` statement, which dbt-core rejects at runtime with
    "main is not being called during running model" (all five routine-load
    shell tables hit this on first run, 2026-11-17). The branch here emits a
    harmless SELECT 1 as `main` instead.

    GUARD: the adapter drops and recreates on --full-refresh even for
    ignore-mode tables. That must never happen here: these tables are fed by
    Routine Load jobs (dbt_starrocks/models/*_direct.sql etc.) -- a drop
    breaks the loads and a replay double-counts. Under 'ignore' the table is
    left untouched regardless of full-refresh mode.
#}

{% materialization table, adapter='starrocks' %}

  {%- set target_relation = this.incorporate(type='table') -%}

  {%- set properties = config.get('properties', {}) -%}
  {%- set catalog = config.get('catalog', target.catalog) -%}
  {%- set database = config.get('database') or target_relation.schema -%}
  {%- set on_table_exists = config.get('on_table_exists', 'replace') -%}

  {%- if catalog == 'default_catalog' -%}
    {%- set is_external = false -%}
  {%- else -%}
    {%- if not catalog or not database -%}
      {%- set msg -%}
        External tables require both 'catalog' and 'database'
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
    {%- set is_external = true -%}
  {%- endif -%}

  {%- if is_external -%}
    {%- set target_relation = adapter.Relation.create(
        database=catalog,
        schema=database,
        identifier=target_relation.identifier,
        type='table'
    ) -%}
    {%- set existing_relation = load_relation(target_relation) -%}

  {%- else -%}
    {%- set existing_relation = load_cached_relation(this) -%}
  {%- endif -%}

  {%- set sql = model['compiled_code'] -%}

  {{ run_hooks(pre_hooks) }}

  {%- if existing_relation is not none -%}
    {%- if on_table_exists == 'ignore' -%}
      {{ log("Table " ~ target_relation ~ " exists and on_table_exists='ignore'"
             ~ " -- leaving it untouched (routine-load fed: a drop would break the load)", info=True) }}
      {% call statement('main') -%}
        SELECT 1
      {%- endcall %}
      {{ return({'relations': [target_relation]}) }}

    {%- elif should_full_refresh() or on_table_exists == 'replace' -%}
      {{ log("Dropping and recreating table", info=True) }}
      {{ adapter.drop_relation(existing_relation) }}
      {%- set existing_relation = none -%}

    {%- elif on_table_exists == 'append' -%}
      {% call statement('main') -%}
        INSERT INTO {{ target_relation }}
        {{ sql }}
      {%- endcall %}
      {{ return({'relations': [target_relation]}) }}

    {%- else -%}
      {%- set msg -%}
        Unknown on_table_exists strategy: '{{ on_table_exists }}'.
        Valid options: 'replace', 'append', 'ignore'
      {%- endset %}
      {{ exceptions.raise_compiler_error(msg) }}
    {%- endif -%}
  {%- endif -%}

  {%- if existing_relation is none -%}
    {{ log("Creating new table: " ~ target_relation, info=True) }}
    {% call statement('main') -%}
      {{ starrocks__create_table_as(false, target_relation, sql, is_external) }}
    {%- endcall %}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
