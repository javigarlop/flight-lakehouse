{#
  Start of the window an incremental run recomputes: lookback_hours before the newest
  bucket already in the model. Buckets from there on are rebuilt in full from silver and
  merged on the model's unique key. On the first run the model is empty, so it starts
  from the epoch.
#}
{% macro lookback_start(bucket_column) -%}
    (
        select
            coalesce(max({{ bucket_column }}), timestamp '1970-01-01 00:00:00 UTC')
            - interval '{{ var("lookback_hours") }}' hour
        from {{ this }}
    )
{%- endmacro %}

{# Exact nearest-rank percentile of an aggregate, so reruns give the same numbers. #}
{% macro nearest_rank(column, fraction) -%}
    element_at(
        array_sort(array_agg({{ column }})),
        greatest(1, cast(ceiling({{ fraction }} * count({{ column }})) as integer))
    )
{%- endmacro %}
