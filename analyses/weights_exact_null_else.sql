with f_name as (
select first_name
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where first_name is not null
group by first_name
)

, f_initial as (
select first_initial
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where first_initial is not null
group by first_initial
)

, f_null as (
select count(first_name)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, l_name as (
select last_name
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where last_name is not null
group by last_name
)

, l_initial as (
select last_initial
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where last_initial is not null
group by last_initial
)

, l_null as (
select count(last_name)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, dob as (select dob
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where dob is not null
group by dob
)

, dob_null as (
select count(dob)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, ssid2 as (select source_system_id_2
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where source_system_id_2 is not null
group by source_system_id_2
)

, ssid2_null as (
select count(source_system_id_2)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, phone as (select phone
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where phone is not null
group by phone
)

, phone_null as (
select count(phone)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, email as (select email
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where email is not null
group by email
)

, email_null as (
select count(email)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, ssn as (select ssn
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where ssn is not null
group by ssn
)

, ssn_null as (
select count(ssn)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, zip_5 as (select zip_5
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where zip_5 is not null
group by zip_5
)

, zip_5_null as (
select count(zip_5)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, previd as (select prev_source_system_id
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where prev_source_system_id is not null
group by prev_source_system_id
)

, previd_null as (
select count(prev_source_system_id)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, death as (select death_date
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where death_date is not null
group by death_date
)

, death_null as (
select count(death_date)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, sex as (select sex
, count(*) as freq
, sum(freq) over () as tot
, (freq*(freq-1))::float/(tot*(tot - 1)) as random_collision
from {{ ref('silver_empi_input') }}
where sex is not null
group by sex
)

, sex_null as (
select count(sex)/count(*) as non_null_frac
, 1 - non_null_frac as null_frac
, pow(null_frac, 2) as u_both_null
, pow(null_frac, 2) as m_both_null
, 2*non_null_frac*null_frac as u_one_null
, 2*non_null_frac*null_frac as m_one_null
from {{ ref('silver_empi_input') }}
)

, levels as (
select 'first_name' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from f_name

union all

select 'first_name' as field
, 'initial' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from f_initial

union all

select 'first_name' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from f_null

union all

select 'first_name' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from f_null

union all

select 'last_name' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from l_name

union all

select 'last_name' as field
, 'initial' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from l_initial

union all

select 'last_name' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from l_null

union all

select 'last_name' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from l_null

union all 

select 'dob' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from dob

union all

select 'dob' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from dob_null

union all

select 'dob' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from dob_null

union all 

select 'ssid2' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from ssid2

union all

select 'ssid2' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from ssid2_null

union all

select 'ssid2' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from ssid2_null

union all 

select 'phone' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from phone

union all

select 'phone' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from phone_null

union all

select 'phone' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from phone_null

union all 

select 'email' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from email

union all

select 'email' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from email_null

union all

select 'email' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from email_null

union all 

select 'ssn' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from ssn

union all

select 'ssn' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from ssn_null

union all

select 'ssn' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from ssn_null

union all 

select 'zip_5' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from zip_5

union all

select 'zip_5' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from zip_5_null

union all

select 'zip_5' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from zip_5_null

union all 

select 'previd' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from previd

union all

select 'previd' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from previd_null

union all

select 'previd' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from previd_null

union all 

select 'death' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from death

union all

select 'death' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from death_null

union all

select 'death' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from death_null

union all 

select 'sex' as field
, 'exact' as cmp_level
, sum(random_collision) as u_value
, 1 as m_value
, m_value/nullif(u_value, 0) as likelihood
, ln(likelihood) as log_likelihood
from sex

union all

select 'sex' as field
, 'both_null' as cmp_level
, u_both_null as u_value
, m_both_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from sex_null

union all

select 'sex' as field
, 'one_null' as cmp_level
, u_one_null as u_value
, m_one_null as m_value
, decode(u_value, 0, 1, m_value/u_value) as likelihood
, ln(likelihood) as log_likelihood
from sex_null
)

select *
from levels

union all

select 'first_name' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'first_name'
group by field

union all

select 'last_name' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'last_name'
group by field

union all

select 'dob' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'dob'
group by field

union all

select 'ssid2' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'ssid2'
group by field

union all

select 'phone' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'phone'
group by field

union all

select 'email' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'email'
group by field

union all

select 'ssn' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'ssn'
group by field

union all

select 'zip_5' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'zip_5'
group by field

union all

select 'previd' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'previd'
group by field

union all

select 'death' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'death'
group by field

union all

select 'sex' as field
, 'else' as cmp_level
, 1 - sum(u_value) as u_value_else
, 1 - sum(u_value) as m_value_else
, decode(u_value_else, 0, 1, m_value_else/u_value_else) as likelihood_else
, ln(likelihood_else) as log_likelihood
from levels
where field = 'sex'
group by field