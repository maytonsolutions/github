select *
from hl7app.adt_msg_queue_sanmateo0531
where visit_number = '000006757457'
order by system_timestamp;

select location, count(distinct visit_number)
from hl7app.adt_msg_queue_hendrick
where processing_status = 'd'
and discharge_datetime >= '20180409000000'
and discharge_datetime < '20180410000000'
group by location;

select count(*)
from hl7app.adt_msg_queue_hendrick
order by system_timestamp desc;

select distinct visit_type
from hl7app.adt_msg_queue_hendrick;

select distinct language
from hl7app.adt_msg_queue_seneca;

show events;

show create event  hl7_export_records_wisehealth_IP_0200;

drop event hl7_export_records_hendrick_1630;

ALTER EVENT hl7_export_records_akron0522_1525  DISABLE;

truncate table hl7app.adt_msg_queue_hendrick;

select *
from hl7app.adt_msg_queue_wisehealth
where visit_type = 'I'
and processing_status  = 'r'
and msg_type = 'A03'
order by system_timestamp desc;

select count(*)
from hl7app.adt_msg_queue_mainemedctr;

update hl7app.adt_msg_queue_mainemedctr
set processing_status = 'f';

select *
from hl7app.adt_msg_queue_mainemedctr
where processing_status = 'r'
order by system_timestamp desc;


delete from 
hl7app.adt_msg_queue_mainemedctr
where processing_status = 'r';

SET @OutputPath := 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads';
SET @fullOutputPath := CONCAT(@OutputPath,'/','filename.csv');
SET @fullOutputPath2 := CONCAT(@OutputPath,'/','filename2.csv');

set @q1 := concat("SELECT '0' INTO OUTFILE ",@fullOutputPath,
" FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '\"'
FROM hl7app.adt_msg_queue_ohsu ");

set @q2 := concat("SELECT '1' INTO OUTFILE ",@fullOutputPath2,
" FIELDS TERMINATED BY ',' 
OPTIONALLY ENCLOSED BY '\"'
FROM hl7app.adt_msg_queue_ohsu ");

prepare s1 from @q1;
execute s1;deallocate prepare s1;

prepare s1 from @q2;
execute s1;deallocate prepare s1;

select '1' INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/OHSU.OK';
