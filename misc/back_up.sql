select *
into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/ALL_HL7_20170720000000'
FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
LINES TERMINATED BY '\n'
FROM hl7app.adt_msg_queue
where system_timestamp < '20170720000000';

select count(*)
from hl7app.adt_msg_queue
where system_timestamp < '20170720000000';

delete from hl7app.adt_msg_queue
where system_timestamp < '20170720000000';
