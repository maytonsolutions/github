select location, count(distinct visit_number)
from hl7app.adt_msg_queue_kalispell
where processing_status = 'd'
and discharge_datetime >= '20180424000000'
and discharge_datetime < '20180425000000'
group by location;

select msg_controlid, msg_type, msg_send_timestamp, discharge_datetime
from hl7app.adt_msg_queue_kalispell
where processing_status = 'd'
and discharge_datetime >= '20180424000000'
and discharge_datetime < '20180425000000'
and location = 'C.FVOC'
group by location;

select *
from hl7app.adt_msg_queue_kalispell
where processing_status = 'd'
and discharge_datetime >= '20180411000000'
and discharge_datetime < '20180412000000'
and location = 'K.DIAB'
group by location;

select *
from hl7app.adt_msg_queue_kalispell
where visit_number = 'V00020298093';

select *
from hl7app.adt_msg_queue_wisehealth
where visit_type = 'I';

select *
from hl7app.adt_msg_queue_kalispell
limit 10;

select count(*) 
from hl7app.adt_msg_queue_chdallas;

select * 
from hl7app.adt_msg_queue_chdallas
where mrn = '1577949';


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
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_HMH0530_STAGE_HL7_20180420000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180420000000';

select count(*)
from hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180420000000'
limit 10;

delete from hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180420000000';

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_HENFRICK_STAGE_HL7_20180426000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_hendrick
where system_timestamp < '20180426000000';

select count(*)
from hl7app.adt_msg_queue_hendrick
where system_timestamp < '20180426000000'
limit 10;

delete from hl7app.adt_msg_queue_hendrick
where system_timestamp < '20180426000000';

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_SANMATEO0531_STAGE_HL7_20180410000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_sanmateo0531
where system_timestamp < '20180410000000';

select count(*)
from hl7app.adt_msg_queue_sanmateo0531
where system_timestamp < '20180410000000'
limit 10;

delete from hl7app.adt_msg_queue_sanmateo0531
where system_timestamp < '20180410000000';

select distinct location
from hl7app.adt_msg_queue_chdallas;

select *
from hl7app.adt_msg_queue_chdallas
where visit_number = '95339825';

select *
from hl7app.adt_msg_queue_mainemedctr
limit 10;

truncate table hl7app.adt_msg_queue_mainemedctr;

truncate table hl7app.adt_msg_queue_hendrick;

select count(*)
from hl7app.adt_msg_queue_hendrick;

select *
from hl7app.adt_msg_queue_hendrick
order by system_timestamp desc
limit 200;


show processlist;

show events;

drop event hl7_export_records_ohsu_1700;

alter event hl7_export_records_akron0522_0925 disable;

alter event hl7_export_records_akron0522_1525 disable;

alter event hl7_export_records_driscollch_0855 disable;

alter event hl7_export_records_driscollch_1455 disable;

alter event hl7_export_records_metrohealth_0910 disable;

alter event hl7_export_records_metrohealth_1510 disable;

ALTER TABLE hl7app.adt_msg_queue_hendrick MODIFY admit_source VARCHAR(60);

select distinct visit_type
from hl7app.adt_msg_queue_seneca
order by system_timestamp desc;

truncate table hl7app.adt_msg_queue_seneca;

select distinct location
from hl7app.adt_msg_queue_wisehealth;

delete from hl7app.adt_msg_queue_seneca
where system_timestamp <= '20180506180714';

select *
from hl7app.adt_msg_queue_sanmateo0531
where msg_type = 'A08'
and processing_status = 'd'
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_sanmateo0531
where msg_type = 'A04'
and processing_status = 'd'
order by system_timestamp desc;

SHOW GLOBAL VARIABLES like '%MAX%';

SET GLOBAL max_allowed_packet = 33554432;

SET GLOBAL event_scheduler = ON;

select *
from hl7app.adt_msg_queue_mainemedctr
where processing_status = 'r'
order by system_timestamp desc;

delete from hl7app.adt_msg_queue_mainemedctr
where system_timestamp > '20180507000000';

truncate table hl7app.adt_msg_queue_mainemedctr;

select distinct visit_type
from hl7app.adt_msg_queue_wrmc;



select *
from hl7app.adt_msg_queue_wrmc
order by system_timestamp desc;

truncate table hl7app.adt_msg_queue_wrmc;

select *
from hl7app.adt_msg_queue_seneca
order by system_timestamp desc;


select distinct death_indicator
from hl7app.adt_msg_queue_chdallas;

select *
from hl7app.adt_msg_queue_ohsu
order by system_timestamp desc
limit 1000;

alter table hl7app.adt_msg_queue_ohsu add column service_indicator03 varchar(60) after service_indicator02;

truncate table hl7app.adt_msg_queue_ohsu;





