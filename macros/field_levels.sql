{%- macro field_levels(alias_left = 'a' , alias_right = 'b') %}
{{ alias_left }}.source_system_id as l_id
, {{ alias_right }}.source_system_id as r_id
, {{ alias_left }}.prev_source_system_id as l_prev_id
, {{ alias_right }}.prev_source_system_id as r_prev_id
, {{ alias_left }}.source_system_id_2 as l_id_2
, {{ alias_right }}.source_system_id_2 as r_id_2
, {{ alias_left }}.surrogate_key as l_surrogate_key
, {{ alias_right }}.surrogate_key as r_surrogate_key
, {{ alias_left }}.last_updated as l_last_updated
, {{ alias_right }}.last_updated as r_last_updated
, {{ alias_left }}.first_name as l_first_name 
, {{ alias_right }}.first_name as r_first_name
, {{ alias_left }}.first_initial as l_first_initial
, {{ alias_right }}.first_initial as r_first_initial
, {{ alias_left }}.first_name_soundex as l_first_name_soundex
, {{ alias_right }}.first_name_soundex as r_first_name_soundex
, {{ alias_left }}.last_name as l_last_name
, {{ alias_right }}.last_name as r_last_name
, {{ alias_left }}.last_name_soundex as l_last_name_soundex
, {{ alias_right }}.last_name_soundex as r_last_name_soundex
, {{ alias_left }}.dob as l_dob
, {{ alias_right }}.dob as r_dob
, {{ alias_left }}.birth_year as l_birth_year
, {{ alias_right }}.birth_year as r_birth_year
, {{ alias_left }}.sex as l_sex
, {{ alias_right }}.sex as r_sex
, {{ alias_left }}.race as l_race
, {{ alias_right }}.race as r_race
, {{ alias_left }}.address_line_1 as l_address_line_1
, {{ alias_right }}.address_line_1 as r_address_line_1
, {{ alias_left }}.address_line_2 as l_address_line_2
, {{ alias_right }}.address_line_2 as r_address_line_2
, {{ alias_left }}.full_address as l_full_address
, {{ alias_right }}.full_address as r_full_address
, {{ alias_left }}.city as l_city
, {{ alias_right }}.city as r_city
, {{ alias_left }}.state as l_state
, {{ alias_right }}.state as r_state
, {{ alias_left }}.zip_5 as l_zip_5
, {{ alias_right }}.zip_5 as r_zip_5
, {{ alias_left }}.zip_4 as l_zip_4
, {{ alias_right }}.zip_4 as r_zip_4
, {{ alias_left }}.phone as l_phone
, {{ alias_right }}.phone as r_phone
, {{ alias_left }}.email as l_email
, {{ alias_right }}.email as r_email
, {{ alias_left }}.death_date as l_death_date
, {{ alias_right }}.death_date as r_death_date
, {{ alias_left }}.ssn as l_ssn
, {{ alias_right }}.ssn as r_ssn
, case 
    when {{ alias_left }}.source_system_id_2 = {{ alias_right }}.source_system_id_2 then 'EXACT'    
    when {{ alias_left }}.source_system_id_2 <> {{ alias_right }}.source_system_id_2 then 'ELSE'
end as ssid2_l
, case 
    when {{ alias_left }}.phone = {{ alias_right }}.phone then 'EXACT'
    when {{ alias_left }}.phone <> {{ alias_right }}.phone then 'ELSE'
end as phone_l
, case 
    when {{ alias_left }}.email = {{ alias_right }}.email then 'EXACT'
    when {{ alias_left }}.email <> {{ alias_right }}.email then 'ELSE'
end as email_l
, case 
    when {{ alias_left }}.ssn = {{ alias_right }}.ssn then 'EXACT'
    when {{ alias_left }}.ssn <> {{ alias_right }}.ssn then 'ELSE'
end as ssn_l
, case 
    when {{ alias_left }}.last_name = {{ alias_right }}.last_name 
        or split_part(split_part({{ alias_left }}.last_name, '-', 1), ' ', 1) = split_part(split_part({{ alias_right }}.last_name, '-', 1), ' ', 1) then 'EXACT'
    when {{ alias_left }}.last_name_soundex = {{ alias_right }}.last_name_soundex
        or editdistance({{ alias_left }}.last_name, {{ alias_right }}.last_name) <= 
        CASE
            WHEN LEAST(length({{ alias_left }}.last_name), length({{ alias_right }}.last_name)) <= 4 then 0
            WHEN LEAST(length({{ alias_left }}.last_name), length({{ alias_right }}.last_name)) <= 8 then 1
            ELSE 2
        END then 'FUZZY'
    when {{ alias_left }}.last_initial = {{ alias_right }}.last_initial then 'INITIAL'    
    when {{ alias_left }}.last_name <> {{ alias_right }}.last_name then 'ELSE'
end as last_name_l
, case 
    when {{ alias_left }}.first_name = {{ alias_right }}.first_name then 'EXACT'
    when {{ alias_left }}.first_initial = {{ alias_right }}.first_initial then 'INITIAL'    
    when {{ alias_left }}.first_name <> {{ alias_right }}.first_name then 'ELSE'
end as first_name_l 
, case 
    when {{ alias_left }}.dob = {{ alias_right }}.dob then 'EXACT'
    when {{ alias_left }}.dob = DATEADD(day, 1, {{ alias_right }}.dob)
        OR {{ alias_left }}.dob = DATEADD(day, -1, {{ alias_right }}.dob)
        OR (DAY({{ alias_left }}.dob) = MONTH({{ alias_right }}.dob) AND MONTH({{ alias_left }}.dob) = DAY({{ alias_right }}.dob) AND {{ alias_left }}.birth_year = {{ alias_right }}.birth_year)
        OR (DAY({{ alias_left }}.dob) = DAY({{ alias_right }}.dob) AND MONTH({{ alias_left }}.dob) = MONTH({{ alias_right }}.dob) AND ABS({{ alias_left }}.birth_year - {{ alias_right }}.birth_year) = 1) then 'FUZZY'    
    when {{ alias_left }}.dob <> {{ alias_right }}.dob then 'ELSE'
end as dob_l 
, case 
    when {{ alias_left }}.zip_5 = {{ alias_right }}.zip_5 then 'EXACT'
    when {{ alias_left }}.zip_5 <> {{ alias_right }}.zip_5 then 'ELSE'
end as zip_l
, case 
    when {{ alias_left }}.death_date = {{ alias_right }}.death_date then 'EXACT'
    when {{ alias_left }}.death_date <> {{ alias_right }}.death_date then 'ELSE'
end as death_date_l
, case 
    when {{ alias_left }}.sex = {{ alias_right }}.sex then 'EXACT'
    when {{ alias_left }}.sex <> {{ alias_right }}.sex then 'ELSE'
end as sex_l
, case 
    when {{ alias_left }}.full_address = {{ alias_right }}.full_address then 'EXACT' 
    when editdistance({{ alias_left }}.full_address, {{ alias_right }}.full_address) <= 3 then 'FUZZY_3'
    when editdistance({{ alias_left }}.full_address, {{ alias_right }}.full_address) <= 5 then 'FUZZY_5'   
    when {{ alias_left }}.full_address <> {{ alias_right }}.full_address then 'ELSE'
end as full_address_l
{% endmacro %}