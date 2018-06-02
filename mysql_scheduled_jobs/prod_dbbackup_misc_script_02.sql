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

drop event hl7_export_records_cmh_1420;

select *
from hl7app.adt_msg_queue_uhs0516
where visit_number = '148581478';

select *
from hl7app.adt_msg_queue_uhs0516
where msg_controlid = '64785_21340_VI';

select count(*)
from hl7app.adt_msg_queue_uhs0516_file
where (processing_status = 'd' OR processing_status = 'c');

select count(*)
from hl7app.adt_msg_queue_uhs0516_file
where (processing_status = 'd' and msg_type = 'A03');

delete 
from hl7app.adt_msg_queue_uhs0516_file
where (processing_status = 'd' OR processing_status = 'c');


select count(*)
from hl7app.adt_msg_queue_hmh0530;


kill 3958078;

show processlist;

kill 414364;

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

select count(*)
from hl7app.adt_msg_queue_kalispell;

show create event hl7_export_records_prevea_0830;

alter event hl7_export_records_uhs0516_1550 enable;


truncate table hl7app.adt_msg_queue_kalispell;

select *
from hl7app.adt_msg_queue_kalispell;

SHOW GLOBAL VARIABLES like '%job%';

SET GLOBAL event_scheduler = ON;

SET GLOBAL max_allowed_packet = 33554432;

SELECT * FROM INFORMATION_SCHEMA.EVENTS
WHERE EVENT_NAME = 'hl7_export_records_pennstate_0840'
AND EVENT_SCHEMA = 'hl7app';

select *
from hl7app.adt_msg_queue
where customer_id = 'CHCOLORADO'
and processing_status <> 'd'
order by system_timestamp desc;


select count(*)
from hl7app.adt_msg_queue_comhlthnet0432;

select *
from hl7app.adt_msg_queue
where customer_id = 'METROHEALTH'
and email_address = 'sonnyd51@hotmail.com';

select *
from hl7app.adt_msg_queue_comhlthnet0432
where visit_number = '10079197181'
OR visit_number = '10079197147';

select *
from hl7app.adt_msg_queue_comhlthnet0432
where visit_number = '10079197147';


select * 
from hl7app.adt_msg_queue_comhlthnet0432 
WHERE processing_status = 'd'
AND (customer_id = 'CHA50003')
AND visit_number = '10079197147'
AND msg_type = 'A03'
AND system_timestamp < now() - 10
AND (location = 'ED-E' OR location = 'ED-N' OR location = 'ED-S')
AND MRN NOT IN (
    SELECT v_MRN 
	FROM (
        SELECT distinct MRN as v_MRN 
        FROM hl7app.adt_msg_queue_comhlthnet0432
    	WHERE (location = 'COH-E' OR location = 'COH-N' OR location = 'COH-S')
        AND msg_type = 'A04'
        AND customer_id = 'CHA50003'
		AND system_timestamp > 20180424064444 - INTERVAL 1 DAY
		) as s
);


select distinct location
from hl7app.adt_msg_queue_wisehealth;

select *
from hl7app.adt_msg_queue_wisehealth
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_choa
limit 10;

select count(*)
from hl7app.adt_msg_queue_uhs0516_file;


select distinct visit_type
from hl7app.adt_msg_queue_comhlthnet0432
where customer_id = 'CHA';




update hl7app.adt_msg_queue
set processing_status = 'r'
where customer_id = 'METROHEALTH'
and email_address = 'sonnyd51@hotmail.com';

select location, count(distinct visit_number)
from hl7app.adt_msg_queue_uhs0516
where msg_type = 'A03'
and processing_status = 'd'
group by location;

select *
from hl7app.adt_msg_queue_uhs0516
where msg_type = 'A03'
and processing_status = 'd'
and location = 'B103'
group by location;


select *
from hl7app.adt_msg_queue_comhlthnet0432
where visit_number='10079477475';

update hl7app.adt_msg_queue_comhlthnet0432
set processing_status = 'r'
where visit_number='10079477475';

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'B301'
and msg_type = 'A03'
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'H405'
and msg_type = 'A03'
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'H405'
and visit_number =  '149254883';

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'H805'
and visit_number = '149248945';

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'M708';

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'B301'
and visit_number = '149240687';

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'B301'
and processing_status = 'd'
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'B301'
and visit_number = '149178070';

select *
from hl7app.adt_msg_queue_uhs0516
where location = 'B301'
and visit_number = '149178073';


select *
from hl7app.adt_msg_queue_uhs0516
where location = 'B145'
and msg_type = 'A03'
and processing_status = 'd';

select *
from hl7app.adt_msg_queue_uhs0516
where  msg_type = 'A03'
and processing_status = 'r';

select *
from hl7app.adt_msg_queue_uhs0516
where address1 = '2404 S CALAVERAS APT 6107';

select distinct sending_facility_name
from hl7app.adt_msg_queue_hendrick;

select distinct location
from hl7app.adt_msg_queue_hendrick;

select distinct visit_type
from hl7app.adt_msg_queue_comhlthnet0432
where customer_id = 'CHA50003';

select distinct visit_number
from hl7app.adt_msg_queue_comhlthnet0432
where customer_id = 'CHA50003'
AND msg_type <> 'A03'
AND (visit_type = 'INPATIENT' OR visit_type = 'OUTPT IN BED' OR visit_type = 'OBSERVATION')
AND visit_number not in (
    select visit_number 
    from hl7app.adt_msg_queue_comhlthnet0432
    where customer_id = 'CHA50003'
    AND msg_type = 'A03'
    AND (visit_type = 'INPATIENT' OR visit_type = 'OUTPT IN BED' OR visit_type = 'OBSERVATION')
);

select *
from hl7app.adt_msg_queue_comhlthnet0432
where customer_id = 'CHA50003'
AND msg_type <> 'A03'
AND (visit_type = 'INPATIENT')
AND visit_number not in (
    select visit_number 
    from hl7app.adt_msg_queue_comhlthnet0432
    where customer_id = 'CHA50003'
    AND msg_type = 'A03'
    AND (visit_type = 'INPATIENT')
);


select distinct language
from hl7app.adt_msg_queue_seneca;

select *
from hl7app.adt_msg_queue_comhlthnet0432
where location like 'COH%'
order by system_timestamp desc;

select * from hl7app.adt_msg_queue_choa amq
        INNER JOIN (
            select adt.visit_number, MIN(adt.system_timestamp) as minTS 
            from adt_msg_queue_choa adt
            where msg_type = 'A08'
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = minTS
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'CHOA')
        AND amq.msg_type = 'A08';

