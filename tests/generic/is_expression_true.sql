{% test is_expression_true (model,expression) %}
    select
        *
    from 
        {{ model }}

    where not ({{ expression }})

{% endtest %}