show events;

SELECT * FROM INFORMATION_SCHEMA.EVENTS WHERE EVENT_NAME = 'hl7_export_records_chdallas_0935';

drop event hl7app.hl7_export_records_chdallas_1600;

select sending_facility_id, ',' , location, ',', count(distinct visit_number)
from hl7app.adt_msg_queue_cghmc
where discharge_datetime >= '20180227000000'
AND discharge_datetime < '20180228000000'
AND visit_number <> ''
AND processing_status = 'd'
group by sending_facility_id, location;

select distinct visit_number
from hl7app.adt_msg_queue_cghmc
where discharge_datetime >= '20180227000000'
AND discharge_datetime < '20180228000000'
AND sending_facility_id = 'HNAM'
AND msg_type = 'A03'
AND location = 'PED'
AND mrn = '20670';

select *
from  hl7app.adt_msg_queue_sanmateo0531_file
order by discharge_datetime;

select *
from hl7app.adt_msg_queue_hendrick
order by system_timestamp desc
limit 10;

select *
from hl7app.adt_msg_queue_centra
order by system_timestamp desc
limit 30;

update hl7app.adt_msg_queue_centra
set processing_status = 'c'
order by system_timestamp desc
limit 30;

select *
from hl7app.adt_msg_queue_chdallas
where service_indicator01 <> NULL;

select distinct sending_facility_id
from hl7app.adt_msg_queue_choa_file
where customer_id = 'CHOA';

select *
from hl7app.adt_msg_queue_chdallas
where customer_id = 'CHDALLAS'
order by system_timestamp desc;


select *
from hl7app.adt_msg_queue_wisehealth
where (msg_type = 'A03' OR msg_type = 'A04');

---truncate table hl7app.adt_msg_queue_wisehealth;


select count(*) from  hl7app.adt_msg_queue_hmh0530_file;

select *
from hl7app.adt_msg_queue_kalispell
where customer_id = 'KALISPELL50050'
order by system_timestamp desc;

show global variables like '%event%';


drop table hl7app.adt_msg_queue_hmh0530_file;

select distinct sending_facility_id
from hl7app.adt_msg_queue_cghmc;


select distinct location 
from hl7app.adt_msg_queue_chdallas;

select clinic_Name, count(distinct visit_number)
from hl7app.adt_msg_queue_sanmateo0531
where processing_status = 'd'
and customer_id = 'SANMATEO0531'
and discharge_datetime >= '20180315000000'
and discharge_datetime < '20180316000000'
group by clinic_Name;


select *
from hl7app.adt_msg_queue_sanmateo0531_file
WHERE visit_type = 'I'
AND customer_id = 'SANMATEO0531'
AND msg_type = 'A03'
AND discharge_datetime >= '20180118000000'
AND discharge_datetime < '20180125000000';

select *
from hl7app.adt_msg_queue_sanmateo0531_file
where processing_status = 'r';

update hl7app.adt_msg_queue_sanmateo0531_file
set processing_status = 'r'
where processing_status = 'd';

truncate table hl7app.adt_msg_queue_sanmateo0531_file;
        

select msg_type, system_timestamp
from hl7app.adt_msg_queue_cghmc
where visit_number = ''
and msg_type = 'A03'
order by system_timestamp desc;

select *
from hl7app.adt_msg_queue_hendrick
order by system_timestamp desc;

truncate table hl7app.adt;

select visit_type, discharge_datetime
from hl7app.adt_msg_queue_hmh0530_file
where (visit_type = 'I' 
            OR visit_type = 'Inpatient'
            OR visit_type = 'NEWBORN'
            OR visit_type = 'SURGERY ADMIT'
            OR visit_type = 'BOARDER BABY'
            OR visit_type = 'DECEASED - ORGAN DONOR'
			OR visit_type = 'GLOBAL INPATIENT'
            OR visit_type = 'PSYCHIATRIC'
            OR visit_type = 'ALIVE ORGAN DONOR'
            OR visit_type = 'ETAINED BABY'
            OR visit_type = 'NICU'
            OR visit_type = 'RESEARCH INPATIENT'
            OR visit_type = 'SURG ADMIT'
            OR visit_type = 'ORGAN DONAR'
            OR visit_type = 'GLOBAL INPT'
            OR visit_type = 'ALIVE ORGAN'
            OR visit_type = 'DETAINED BAB'
            OR visit_type = 'RESEARCH INP'
            OR visit_type = 'SNF IP')
order by discharge_datetime;

delete 
from hl7app.adt_msg_queue_hmh0530_file
where  (visit_type <> 'I' 
	AND visit_type <> 'Inpatient'
	AND visit_type <> 'NEWBORN'
	AND visit_type <> 'SURGERY ADMIT'
	AND visit_type <> 'BOARDER BABY'
	AND visit_type <> 'DECEASED - ORGAN DONOR'
	AND visit_type <> 'GLOBAL INPATIENT'
	AND visit_type <> 'PSYCHIATRIC'
	AND visit_type <> 'ALIVE ORGAN DONOR'
	AND visit_type <> 'ETAINED BABY'
	AND visit_type <> 'NICU'
	AND visit_type <> 'RESEARCH INPATIENT'
	AND visit_type <> 'SURG ADMIT'
	AND visit_type <> 'ORGAN DONAR'
	AND visit_type <> 'GLOBAL INPT'
	AND visit_type <> 'ALIVE ORGAN'
	AND visit_type <> 'DETAINED BAB'
	AND visit_type <> 'RESEARCH INP'
	AND visit_type <> 'SNF IP');
    
delete 
from hl7app.adt_msg_queue_hmh0530_file
where processing_status = 'd';

select count(*)
from hl7app.adt_msg_queue_mrncidchoa;

select distinct mrn, ',', cid
from hl7app.adt_msg_queue_mrncidchoa;

--truncate table hl7app.adt_msg_queue_mrncidchoa;

select *
from hl7app.adt_msg_queue_sanmateo0531_file
order by discharge_datetime;

SELECT 
 table_schema as `Database`, 
 table_name AS `Table`, 
 round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` 
FROM information_schema.TABLES 
ORDER BY (data_length + index_length) DESC;

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_HL7_Stage_HMH0530_20180210000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180210000000';
