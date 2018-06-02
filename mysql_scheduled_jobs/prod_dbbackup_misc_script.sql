select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_HMH0530_PROD_HL7_20180208000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180208000000';

select count(*)
from hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180218000000'
limit 10;

delete from hl7app.adt_msg_queue_hmh0530
where system_timestamp < '20180228000000';

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_CLDMERCY0425_PROD_HL7_20180208000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_cldmercy0425
where system_timestamp < '20180208000000';

select count(*)
from hl7app.adt_msg_queue_cldmercy0425
where system_timestamp < '20180218000000'
limit 10;

delete from hl7app.adt_msg_queue_cldmercy0425
where system_timestamp < '20180228000000';


select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_OSU0523_PROD_HL7_20180401000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_osu0523
where system_timestamp < '20180401000000';

select count(*)
from hl7app.adt_msg_queue_osu0523
where system_timestamp < '20180401000000'
limit 10;

delete from hl7app.adt_msg_queue_osu0523
where system_timestamp < '20180401000000';

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_PROD_HL7_COMHLTHNET20180430000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_comhlthnet0432
where system_timestamp < '20180430000000';

select count(*)
from hl7app.adt_msg_queue_comhlthnet0432
where system_timestamp < '20180430000000'
limit 10;

delete from hl7app.adt_msg_queue_comhlthnet0432
where system_timestamp < '20180430000000';

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_PROD_HL7_20180523000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue
where system_timestamp < '20180523000000';

select count(*)
from hl7app.adt_msg_queue
where system_timestamp < '20180523000000'
limit 10;

delete from hl7app.adt_msg_queue
where system_timestamp < '20180523000000';

select *
from hl7app.adt_msg_queue
where customer_id = 'METROHEALTH'
AND address1 = '3257 TWAIN CIR';

update hl7app.adt_msg_queue
set processing_status = 'r'
where customer_id = 'METROHEALTH'
AND address1 = '3257 TWAIN CIR';



select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_UHS0516_PROD_HL7_20180522000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_uhs0516
where system_timestamp < '20180522000000';

select count(*)
from hl7app.adt_msg_queue_uhs0516
where system_timestamp < '20180522000000'
limit 10;


delete from hl7app.adt_msg_queue_uhs0516
where system_timestamp < '20180522000000';

select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_AWS_UHS0516_PROD_HL7_20180522000000_PROCESSESD'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
ESCAPED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue_uhs0516
where processing_status = 'd '
OR processing_status = 'c';

select count(*)
from hl7app.adt_msg_queue_uhs0516
where processing_status = 'd '
OR processing_status = 'c';



delete from hl7app.adt_msg_queue_uhs0516
where processing_status = 'd '
OR processing_status = 'c';



show processlist;

