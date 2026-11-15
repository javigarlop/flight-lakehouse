{# Fails when two rows share the same values in all the given columns. #}
{% test unique_combination(model, columns) %}
    select {{ columns | join(", ") }}
    from {{ model }}
    group by {{ columns | join(", ") }}
    having count(*) > 1
{% endtest %}
