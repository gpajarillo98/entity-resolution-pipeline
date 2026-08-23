{#
    dbt's default generate_schema_name macro APPENDS a custom schema to
    the target's default schema rather than replacing it -- so a seed
    configured with +schema: raw actually lands in "main_raw" on DuckDB
    but "analytics_raw" on Snowflake (target schema + "_raw"). That's
    fine as long as only one target is ever used, but this project
    deliberately runs the SAME dbt_utils.generate_surrogate_key -> source()
    reference against both DuckDB (dev) and Snowflake (prod), and the
    staging models' source() calls need a schema name that's identical
    and predictable across both.

    This is dbt Labs' own documented override for exactly this situation:
    a custom schema name now means literally that schema, full stop,
    regardless of which target is active. See sources.yml -- it points
    at schema "raw", and this macro is what guarantees "raw" is where
    both `dbt seed` (DuckDB) and load_to_snowflake.py (Snowflake) will
    actually land data.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {{ default_schema }}

    {%- else -%}

        {{ custom_schema_name | trim }}

    {%- endif -%}

{%- endmacro %}
