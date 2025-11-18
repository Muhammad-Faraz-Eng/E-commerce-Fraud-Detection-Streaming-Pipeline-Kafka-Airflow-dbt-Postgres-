{% macro flatten_data(data , col_name)%}
        {{data}} ->> '{{col_name}}'
{% endmacro %}