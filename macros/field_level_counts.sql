{% macro field_level_counts(m_u_selection, totals_alias = 'tot', bucketed_alias = 'bucketed') %}

{% set fields = [
    'first_name',
    'last_name',
    'ssid2',
    'phone',
    'email',
    'ssn', 
    'dob',
    'sex',
    'zip',
    'death_date',
    'full_address',] %}

{%- for field in fields -%}
    select '{{ field }}' as field
    , {{ field }}_l as cmp_level
    , count(*) as cmp_count
    , count(*)::float/{{ totals_alias }}.{{ field }}_ct as {{ m_u_selection }}_value
    from {{ bucketed_alias }} 
    cross join {{ totals_alias }}
    where {{ field }}_l is not null
    group by {{ field }}_l, {{ totals_alias }}.{{ field }}_ct
    {% if not loop.last %}
    union all
    {% endif %}
{% endfor -%}

{% endmacro %}