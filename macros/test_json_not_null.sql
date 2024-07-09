{% macro test_json_property_exists(model, column_name, property) %}

select
    *
from {{ model }}
where JSONExtractRaw({{ column_name }}, '{{ property }}') is null
   or JSONExtractRaw({{ column_name }}, '{{ property }}') = 'null'
   or trim(JSONExtractRaw({{ column_name }}, '{{ property }}')) = ''
   or JSONExtractRaw({{ column_name }}, '{{ property }}') = '""'

{% endmacro %}