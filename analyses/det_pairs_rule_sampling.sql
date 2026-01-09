/*
This analysis is to help draw random sample pairs from each deterministic matching rule
for manual review and labeling.  There are four auto-scorcing rules (1-4) that have been verified.  
Use this to autoscore the sample for definite matches (rule #1) and then quickly check rules 2-4, which should 
all be matches.  Then, manually score #5.  This allows for calculation of precision by rule in conjunction
with prevalence from the num_pairs_by_rule analysis.  

*/

with random_sample as (
select *
from {{ref('det_pairs')}}
sample (100 ROWS)
where deterministic_rule = 'name_dob_sex_exact'

union all

select *
from {{ref('det_pairs')}}
sample (100 ROWS)
where deterministic_rule = 'ssn_dob_sex'

union all

select *
from {{ref('det_pairs')}}
sample (100 ROWS)
where deterministic_rule = 'first_name_dob_sex_zip_5'

union all

select *
from {{ref('det_pairs')}}
sample (100 ROWS)
where deterministic_rule = 'firstini_last_dob_sex_zip_5'

union all

select *
from {{ref('det_pairs')}}
sample (100 ROWS)
where deterministic_rule = 'name_phone_zip_5'

union all

select *
from {{ref('det_pairs')}}
sample (100 ROWS)
where deterministic_rule = 'name_email_zip_5'
)

select *
-- definite matches
, CASE
    WHEN l_first_name = r_first_name
        AND l_last_name = r_last_name
        AND l_dob = r_dob
        --AND l_sex = r_sex -- removed SEX as it always matches even in non-matches or nulls
        AND l_city = r_city
        AND l_state = r_state
        AND l_zip_5 = r_zip_5
        AND l_full_address = r_full_address
        AND NOT (l_phone is not null and r_phone is not null and l_phone <> r_phone)
        AND NOT (l_email is not null and r_email is not null and l_email <> r_email)
        AND NOT (l_ssn is not null and r_ssn is not null and l_ssn <> r_ssn)
        AND NOT (l_death_date is not null and r_death_date is not null and l_death_date <> r_death_date) THEN 1
-- just need to check first name to see if its a misspelling, intitial, etc
    WHEN l_first_initial = r_first_initial --allowing some fuzziness in firts name
        AND l_last_name = r_last_name
        AND l_dob = r_dob
        AND l_sex = r_sex
        AND l_city = r_city
        AND l_state = r_state
        AND l_zip_5 = r_zip_5
        AND l_full_address = r_full_address
        AND NOT (l_phone is not null and r_phone is not null and l_phone <> r_phone)
        AND NOT (l_email is not null and r_email is not null and l_email <> r_email)
        AND NOT (l_ssn is not null and r_ssn is not null and l_ssn <> r_ssn)
        AND NOT (l_death_date is not null and r_death_date is not null and l_death_date <> r_death_date) THEN 2
-- just need to check last name to see if its a misspelling, suffix, etc.
    WHEN l_first_name = r_first_name
        AND l_last_name_soundex = r_last_name_soundex -- allowing some fuzziness in last name
        AND l_dob = r_dob
        AND l_sex = r_sex
        AND l_city = r_city
        AND l_state = r_state
        AND l_zip_5 = r_zip_5
        AND l_full_address = r_full_address
        AND NOT (l_phone is not null and r_phone is not null and l_phone <> r_phone)
        AND NOT (l_email is not null and r_email is not null and l_email <> r_email)
        AND NOT (l_ssn is not null and r_ssn is not null and l_ssn <> r_ssn)
        AND NOT (l_death_date is not null and r_death_date is not null and l_death_date <> r_death_date) THEN 3
-- just need to check last name and addy to see if its a misspelling, suffix, etc.
    WHEN l_first_name = r_first_name
        AND l_last_name_soundex = r_last_name_soundex -- allowing some fuzziness in last name
        AND l_dob = r_dob
        AND l_sex = r_sex
        AND l_city = r_city
        AND l_state = r_state
        AND l_zip_5 = r_zip_5
        AND NOT (l_phone is not null and r_phone is not null and l_phone <> r_phone)
        AND NOT (l_email is not null and r_email is not null and l_email <> r_email)
        AND NOT (l_ssn is not null and r_ssn is not null and l_ssn <> r_ssn)
        AND NOT (l_death_date is not null and r_death_date is not null and l_death_date <> r_death_date) THEN 4
    ELSE 5
END AS true_label
-- for debugging lengths, uncomment to troubleshoot any unexpected mismatches
-- , length(l_first_name)
-- , length(r_first_name)
-- , length(l_last_name)
-- , length(r_last_name)
-- , length(l_address_line_1)
-- , length(r_address_line_1)
-- , length(l_address_line_2)
-- , length(r_address_line_2)
-- , length(l_full_address)
-- , length(r_full_address)
-- , length(l_city)
-- , length(r_city)
-- , length(l_state)
-- , length(r_state)
-- , length(l_dob)
-- , length(r_dob)
-- , length(l_sex)
-- , length(r_sex)
-- , length(l_zip_5)
-- , length(r_zip_5)
-- , length(l_phone)
-- , length(r_phone)
-- , length(l_email)
-- , length(r_email)
-- , length(l_ssn)
-- , Length(r_ssn)
-- , length(l_death_date)
-- , length(r_death_date)
from random_sample