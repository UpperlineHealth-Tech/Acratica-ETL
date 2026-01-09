with raw_weights as (
select count(*) as total_records
, round(log(10, count(distinct first_name)), 2) as first_name_freq
, round(log(10, count(distinct first_initial)), 2) as first_initial_freq
, round(log(10, count(distinct last_name)), 2) as last_name_freq
, round(log(10, count(distinct last_initial)), 2) as last_initial_freq
, round(log(10, count(distinct last_name_soundex)), 2) as last_name_soundex_freq
, round(log(10, count(distinct dob)), 2) as dob_freq
, round(log(10, count(distinct sex)), 2) as sex_freq
, round(log(10, count(distinct zip_5)), 2) as zip_freq
, round(log(10, count(distinct phone)), 2) as phone_freq
, round(log(10, count(distinct email)), 2) as email_freq
, round(log(10, count(distinct death_date)), 2) as death_date_freq
, round(log(10, count(distinct ssn)), 2) as ssn_freq
, round(log(10, count(distinct source_system_id_2)), 2) as ssid2_freq
, round(log(10, count(distinct prev_source_system_id)), 2) as prev_id_freq
, round(log(10, count(distinct split_part(split_part(last_name, '-', 1), ' ', 1))), 2) as last_split_freq
from {{ref('silver_empi_input')}} )

, final_weights as (
select 
round(2*first_name_freq) as first_name_pts
, round(2*first_initial_freq) as first_initial_pts
, round(2*last_name_freq) as last_name_pts
, round(2*last_initial_freq) as last_initial_pts
, round(2*last_name_soundex_freq) as last_name_soundex_pts
, round(2*dob_freq) as dob_pts
, round(2*sex_freq) as sex_pts
, round(2*zip_freq) as zip_pts
, round(2*phone_freq) as phone_pts
, round(2*email_freq) as email_pts
, round(2*death_date_freq) as death_date_pts
, round(2*ssn_freq) as ssn_pts
, round(2*ssid2_freq) as ssid2_pts
, round(2*prev_id_freq) as prev_id_pts
, round(2*last_split_freq) as last_split_pts
from raw_weights
)

select *
from final_weights
UNPIVOT (
    points for field in (first_name_pts, first_initial_pts, last_name_pts, last_split_pts, last_name_soundex_pts, last_initial_pts, dob_pts
    , sex_pts, zip_pts, phone_pts, email_pts, death_date_pts, ssn_pts, ssid2_pts, prev_id_pts)
)
order by points desc