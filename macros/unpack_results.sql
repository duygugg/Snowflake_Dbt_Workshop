{% macro unpack_results(results) %}

  {% for res in results -%}
-- This is useful for debugging
--     {{ log(res, info=True) }}
        ('{{ invocation_id }}', '{{run_started_at}}', current_timestamp(), '{{ res.node.unique_id }}', '{{ res.status }}', '{{ res.error if res.error != None else ""}}', '{{ res.skip if res.skip != None else "" }}', '{{ res.fail if res.fail != None else "" }}', {{ res.execution_time }} ){{ "," if not loop.last }}
  {% endfor %}

{% endmacro %}