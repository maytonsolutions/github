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

show events;

drop event hl7_export_records_hendrick_1630;

ALTER EVENT hl7_export_records_akron0522_1525  DISABLE;



