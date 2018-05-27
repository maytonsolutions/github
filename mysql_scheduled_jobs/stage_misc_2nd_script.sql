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