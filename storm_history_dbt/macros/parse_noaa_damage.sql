{% macro parse_noaa_damage(col) %}
    case
        when {{ col }} is null               then null
        when upper({{ col }}) = 'T'          then 1
        when upper({{ col }}) like '%K'      then try_cast(replace(upper({{ col }}), 'K', '') as double) * 1000
        when upper({{ col }}) like '%M'      then try_cast(replace(upper({{ col }}), 'M', '') as double) * 1000000
        when upper({{ col }}) like '%B'      then try_cast(replace(upper({{ col }}), 'B', '') as double) * 1000000000
        else try_cast({{ col }} as double)
    end
{% endmacro %}
