{#-
    Ingestion watermark: the dates of `source_relation` that must be (re)processed.

    A date is pending when its latest ingestion in the source is newer than the `ingested_at`
    already stored for that date in this model. On a full refresh (or the first build) every date
    in the source is pending. Pending dates are then replaced with delete+insert.
-#}
{% macro pending_dates(source_relation, source_date_column, source_ts_column, target_date_column) %}
    select src.{{ source_date_column }} as pending_date
    from {{ source_relation }} as src
    group by src.{{ source_date_column }}
    {% if is_incremental() %}
    having max(src.{{ source_ts_column }}) > coalesce(
        (
            select max(tgt.ingested_at)
            from {{ this }} as tgt
            where tgt.{{ target_date_column }} = src.{{ source_date_column }}
        ),
        '-infinity'::timestamptz
    )
    {% endif %}
{% endmacro %}
