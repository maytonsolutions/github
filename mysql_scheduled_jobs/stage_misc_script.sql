select location, count(distinct visit_number)
from hl7app.adt_msg_queue_kalispell
where processing_status = 'd'
and discharge_datetime >= '20180411000000'
and discharge_datetime < '20180412000000'
group by location;

select *
from hl7app.adt_msg_queue_kalispell
limit 10;

select count(*)
from hl7app.adt_msg_queue_wisehealth
where discharge_datetime >= '20180414000000'
and discharge_datetime < '20180416000000'
and visit_type = 'I'
order by system_timestamp;

select count(*)
from hl7app.adt_msg_queue_sanmateo0531;

select count(*)
from hl7app.adt_msg_queue_hmh0530;

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_HMH0530_STAGE_HL7_20180404000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180404000000';

select count(*)
from hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180404000000'
limit 10;

delete from hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180404000000';


show events;