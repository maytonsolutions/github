select *
from hl7app.adt_msg_queue_uhs0516
where processing_status = 'r'
and (msg_type = 'A03' or msg_type = 'A04');

select count(*)
from hl7app.adt_msg_queue_mrncidchoa;

select *
from hl7app.adt_msg_queue_comhlthnet0432
where visit_number = '10078149328';

select *
from hl7app.adt_msg_queue_comhlthnet0432
where visit_number = '10078332047';

select *
from hl7app.adt_msg_queue_comhlthnet0432
where visit_number = '10078443331';


select *
from hl7app.adt_msg_queue_comhlthnet0432
where mrn = '005477684';

select count(*)
from hl7app.adt_msg_queue;

select distinct sending_facility_id, sending_facility_name
from hl7app.adt_msg_queue
where customer_id = 'DUNCAN';

show events;

drop event hl7_export_records_hendrick_1630;

kill 2721129;

show processlist;

ALTER TABLE hl7app.adt_msg_queue_hendrick MODIFY admit_source VARCHAR(60);

SELECT 
 table_schema as `Database`, 
 table_name AS `Table`, 
 round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` 
FROM information_schema.TABLES 
ORDER BY (data_length + index_length) DESC;

optimize table mirthdb.d_mc38;

select * 
from hl7app.adt_msg_queue_chalabama
where location = '06QNICU'
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_hendrick
where visit_number = '50154101';

update hl7app.adt_msg_queue_hendrick
set processing_status = 'r'
where msg_type = 'A13'
and visit_type <> 'I'
and system_timestamp >= '20180430210000';





