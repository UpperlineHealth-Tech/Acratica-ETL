/*
This script is to check for records created in athena that were created on the same day, 
indicating accidental duplication.  It outputs all records created on the same day for a given empi_id.
*/

with record_clusters as (
select empi_id
, count(*) as records_in_cluster
from {{ref('empi_crosswalk_gold')}}
where source_system = 'ATHENA'
group by empi_id
order by count(*) desc
)

, size_over_n as (
select *
from record_clusters
where records_in_cluster >= 2
)

, source_ids as (
select source_system_id
, empi_id
, count(*) over (partition by empi_id) as records_in_cluster
from {{ref('empi_crosswalk_gold')}}
where empi_id in (select empi_id from size_over_n)
and source_system = 'ATHENA'
)

, last_created as (
select s.source_system_id
, s.empi_id
, s.records_in_cluster
, p.patientid
, p.firstname
, p.lastname
, p.sex
, p.dob
, p.address
, p.city
, p.state
, p.zip
, p.registrationdate, p.registrationdepartmentid, p.testpatientyn, p.lastupdated
, d.departmentid, d.departmentname, d.billingname, d.departmentaddress, d.departmentcity, d.departmentstate, d.departmentzip
, a.operation, a.eventdatetime as created_datetime, a.username
, u.email as user_email
, abs(p.registrationdate - lead(p.registrationdate) over(partition by s.empi_id order by created_datetime desc)) as days_since_last_creation
from source_ids s
join {{ source('ATHENA_EMR', 'PATIENT') }} p
on s.source_system_id = p.patientid
left join athenahealth.athenaone.department d
on p.registrationdepartmentid = d.departmentid
join athenahealth.athenaone.patientaudit a
on p.patientid = a.sourceid
left join athenahealth.athenaone.userprofile u
on a.username = u.username
where a.operation = 'CREATE'
)

, add_prior_flag as (
select *
, case
    when lag(days_since_last_creation) over(partition by empi_id order by created_datetime desc) = 0 then 1
end as keep_prior_record_flag
from last_created 
)

select *
from add_prior_flag
where days_since_last_creation = 0 or keep_prior_record_flag = 1
order by records_in_cluster desc, empi_id, created_datetime desc