{% macro clone_production_schema(prod_schema, dev_schema) %}
    
    {% set clone_query %}
        CREATE OR REPLACE SCHEMA {{ dev_schema }} CLONE {{ prod_schema }};
    {% endset %}

    {% do run_query(clone_query) %}
    {{ log("Cloned schema " ~ prod_schema ~ " to " ~ dev_schema, info=True) }}

{% endmacro %}
