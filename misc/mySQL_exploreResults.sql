select distinct (msg_type)
from hl7app.adt_msg_queue

set global max_allowed_packet=33554432;


select count(distinct visit_number)
from hl7app.adt_msg_queue
where system_timestamp <= '20170310040705'

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;
SELECT count(*) FROM hl7app.adt_msg_queue ;

select distinct msg_type
from hl7app.adt_msg_queue

select count(distinct visit_number)
from  hl7app.adt_msg_queue

select count(t1.visit_number)
from hl7app.adt_msg_queue t1, hl7app.adt_msg_queue t2
where t1.msg_type = 'A04'
and t2.msg_type = 'A16'
and t1.visit_number =  t2.visit_number

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;
SELECT count(*) FROM hl7app.adt_msg_queue ;

select distinct msg_type
from hl7app.adt_msg_queue

select count(distinct visit_number)
from  hl7app.adt_msg_queue
where msg_type = 'R01'

select count(t1.visit_number)
from hl7app.adt_msg_queue t1, hl7app.adt_msg_queue t2
where t1.msg_type = 'A04'
and t2.msg_type = 'A16'
and t1.visit_number =  t2.visit_number

select count(distinct visit_number)
from hl7app.adt_msg_queue
where visit_number not in
(select visit_number
from hl7app.adt_msg_queue
where msg_type = 'A03'
)
and msg_type = 'A02'

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;
SELECT count(*) FROM hl7app.adt_msg_queue
order by system_timestamp desc

select count(*)
from hl7app.adt_msg_queue
where attending_doctor_id is null
or length(attending_doctor_id) <> 10

select attending_doctor_id
from hl7app.adt_msg_queue
where length(attending_doctor_id) <> 10

select *
from

select email_address
from hl7app.adt_msg_queue

select attending_doctor_id
from hl7app.adt_msg_queue

delete from hl7app.adt_msg_queue  where system_timestamp > '20170317000000'


select distinct msg_type
from hl7app.adt_msg_queue

select count(distinct visit_number)
from  hl7app.adt_msg_queue
where msg_type = 'R01'

select count(t1.visit_number)
from hl7app.adt_msg_queue t1, hl7app.adt_msg_queue t2
where t1.msg_type = 'A04'
and t2.msg_type = 'A16'
and t1.visit_number =  t2.visit_number

select count(distinct visit_number)
from hl7app.adt_msg_queue
where visit_number not in
(select visit_number
from hl7app.adt_msg_queue
where msg_type = 'A03'
)
and msg_type = 'A02'

select distinct sending_facility_name
from hl7app.adt_msg_queue

truncate table hl7app.adt_msg_queue

'20170320060624'

'20170320055200'

SELECT order_id,product_name,qty FROM orders
INTO OUTFILE '/tmp/orders.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'

SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;
SELECT count(*) FROM hl7app.adt_msg_queue;

SELECT * FROM hl7app.adt_msg_queue
order by system_timestamp asc;

select count(*)
from hl7app.adt_msg_queue
where attending_doctor_id is null
or length(attending_doctor_id) <> 10;

select *
from hl7app.adt_msg_queue
where length(attending_doctor_id) <> 10;


select email_address
from hl7app.adt_msg_queue;

select attending_doctor_id
from hl7app.adt_msg_queue;

delete from hl7app.adt_msg_queue  where system_timestamp > '20170317000000';


select distinct msg_type
from hl7app.adt_msg_queue;

select count(distinct visit_number)
from  hl7app.adt_msg_queue
where msg_type = 'R01';

select count(t1.visit_number)
from hl7app.adt_msg_queue t1, hl7app.adt_msg_queue t2
where t1.msg_type = 'A04'
and t2.msg_type = 'A16'
and t1.visit_number =  t2.visit_number;

select count(distinct visit_number)
from hl7app.adt_msg_queue
where visit_number not in
(select visit_number
from hl7app.adt_msg_queue
where msg_type = 'A03'
)
and msg_type = 'A02';

select distinct sending_facility_name
from hl7app.adt_msg_queue;

truncate table hl7app.adt_msg_queue

show processlist;

delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN
	    UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;

        SELECT * INTO OUTFILE 'c:/user/yyyhu/hl7/out/output.csv'
        FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        FROM hl7app.adt_msg_queue;


        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;

delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 minute
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN
	    UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;

        SELECT * INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/output.csv'
        FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        FROM hl7app.adt_msg_queue;


        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;

SET @OutputPath := 'Users/jo/Documents';
SET @fullOutputPath := CONCAT(@OutputPath,'/','filename.csv');
SET @fullOutputPath2 := CONCAT(@OutputPath,'/','filename2.csv');

set @q1 := concat("SELECT * INTO OUTFILE ",@fullOutputPath,
" FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
FROM database.tableName");

set @q2 := concat("SELECT * INTO OUTFILE ",@fullOutputPath2,
" FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\"'
FROM database.tableName2");

prepare s1 from @q1;
execute s1;deallocate prepare s1;

prepare s1 from @q2;
execute s1;deallocate prepare s1;

SET @sql_text =
   CONCAT (
       "SELECT 'text_or_yourquery' into outfile '"
       , DATE_FORMAT( NOW(), '%Y%m%d')
       , ".txt'"
    );

PREPARE s1 FROM @sql_text;
EXECUTE s1;
DROP PREPARE s1;

PREPARE s1 FROM @sql_text;
EXECUTE s1;
DROP PREPARE s1;

set GLOBAL event_scheduler = 1


CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN
		    UPDATE LOW_PRIORITY hl7app.adt_msg_queue
          SET processing_status= 'p'
		    WHERE processing_status = 'r'
        AND system_timestamp < now() + 1800;

        SELECT * INTO OUTFILE 'c:/user/yyyhu/hl7/out/output.csv'
        FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        FROM hl7app.adt_msg_queue;


        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		    WHERE processing_status = 'p';


      END |

delimiter ;


delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;


        SET @sql_text_select =
        CONCAT (
         "SELECT * into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/hl7output_"
         , DATE_FORMAT( NOW(), '%Y%m%d')
         , ".csv' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;

delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;


        SET @sql_text_select =
        CONCAT (
         "SELECT * into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/hl7output_"
         , DATE_FORMAT( NOW(), '%Y%m%d')
         , ".csv' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p';"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;


delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;


        SET @sql_text_select =
        CONCAT (
         "SELECT 'msg_controlid', 'sending_facility_id', 'msg_send_timestamp',"
         ,"'system_unique-id', 'customer_id', 'customer_name', 'sending_facility_name', "
         ,"'patient_external_id', 'patient_internal_id',  "
         ," UNION ALL ",
         "SELECT * into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/hl7output_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , ".csv' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p';"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;


set GLOBAL event_scheduler = 1

drop event hl7_export_records;

delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;


        SET @sql_text_select =
        CONCAT (
         "SELECT 'msg_controlid', 'sending_facility_id', 'msg_send_timestamp',"
         ,"'system_unique-id', 'customer_id', 'customer_name', 'sending_facility_name', "
         ,"'patient_external_id', 'patient_internal_id', 'mrn', 'patient_account_number', "
         ,"'patient_last_name','patient_first_name', 'patient_middle_name', 'patient_suffix',"
         ,"'primary_phone_number', 'secondary_phone_number', 'email_address', 'address1',"
         ,"'address2', 'city', 'state', 'zip', 'zip5', 'zip4', 'gender', 'race', 'language',"
         ,"'marital', 'dob', 'visit_number', 'visit_type', 'Visit_date', 'attending_doctor_id',"
         ,"'attending_doctor_id_type', 'attending_doctor_last_name', 'attending_doctor_first_name',"
         ,"'attending_doctor_middle_name', 'attending_doctor_suffix', 'attending_doctor_degree',"
         ,"'primary_payer_id', 'primary_payer_name','primary_plan_type','primary_diagnosis', "
         ,"'primary_diag_coding_system', 'primary_diag_description', 'secondary_daignosis',"
         ,"'secondary_diag_coding_system','secondary_diag_description', "
         ,"'tertiary_diagnosis', 'tertiary_diag_coding_system','tertiary_diag_description',"
         ,"'location', 'admit_datetime', 'discharge_datetime', 'discharge_status', "
         ,"'emergency_severity_index', 'ethnic_group', 'processing_status', 'msg '"
         ," UNION ALL ",
         "SELECT * into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/hl7output_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , ".csv' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p';"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;


delimiter &

CREATE EVENT hl7_export_records
    ON SCHEDULE
      EVERY 1 hour
    COMMENT 'pick up every new records that are more than 30 minutes old'
    DO
      BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND system_timestamp < now() - 1800;


        SET @sql_text_select =
        CONCAT (
         "SELECT 'msg_controlid', 'sending_facility_id', 'msg_send_timestamp',"
         ,"'msg_type', 'system_timestamp',"
         ,"'system_unique_id', 'customer_id', 'customer_name', 'sending_facility_name', "
         ,"'patient_external_id', 'patient_internal_id', 'mrn', 'patient_account_number', "
         ,"'patient_last_name','patient_first_name', 'patient_middle_name', 'patient_suffix',"
         ,"'primary_phone_number', 'secondary_phone_number', 'email_address', 'address1',"
         ,"'address2', 'city', 'state', 'zip', 'zip5', 'zip4', 'gender', 'race', 'language',"
         ,"'marital', 'dob', 'visit_number', 'visit_type', 'Visit_date', 'attending_doctor_id',"
         ,"'attending_doctor_id_type', 'attending_doctor_last_name', 'attending_doctor_first_name',"
         ,"'attending_doctor_middle_name', 'attending_doctor_suffix', 'attending_doctor_degree',"
         ,"'primary_payer_id', 'primary_payer_name','primary_plan_type','primary_diagnosis', "
         ,"'primary_diag_coding_system', 'primary_diag_description', 'secondary_daignosis',"
         ,"'secondary_diag_coding_system','secondary_diag_description', "
         ,"'tertiary_diagnosis', 'tertiary_diag_coding_system','tertiary_diag_description',"
         ,"'location', 'admit_datetime', 'discharge_datetime', 'discharge_status', "
         ,"'emergency_severity_index', 'ethnic_group', 'processing_status', 'msg'"
         ," UNION ALL ",
         "SELECT * into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/hl7output_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , ".csv' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p';"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'p';


      END &

delimiter ;


SELECT  patient_first_name as 'PatientNameGiven',
        patient_middle_name as 'PatientNameSecondGiven',
        patient_last_name as 'PatientNameFamily',
        patient_suffix as 'PatientNameSuffix',
        address1 as 'AddressStreet1',
        address2 as 'AddressStreet2',
        city as 'AddressCity',
        state as 'AddressState',
        zip as 'AddressPostalCode',
        area_code as 'PhoneAreaCityCode',
        local_number as 'PhoneLocalNumber',
        mrn as 'MRN',
        dob as 'DateOfBirth',
        gender as 'AdministrativeSex',
        language as 'PrimaryLanguage',
        race as 'Race',
        ethnic_group as 'EthnicGroup',
        marital as 'MaritalStatus',
        email_address as 'Email',
        visit_type as 'PatientClass',
        sending_facility_name as 'FacilityName',
        '' as 'FacilityNPI',
        '' as 'FacilityNumber',
        visit_number as 'VisitNumber',
        admit_datetime as 'AdmitDateTime',
        discharge_datetime as 'DischargeDateTime',
        '' as 'AdmitSource',
        discharge_status as 'DischargeStatus',
        location as 'DischargeUnitID',
        '' as 'DischargeUnitName',
        '' as 'MSDRG',
        primary_diagnosis as 'DiagnosisPrimaryICD10',
        secondary_diagnosis as 'Diagnosis2ICD10',
        tertiary_diagnosis as 'Diagnosis3ICD10',
        death_indicator as 'IsDeceased',
        '' as 'ICU',
        '' as 'EDAdmit',
        primary_payer_id as 'InsuranceCompanyID',
        primary_payer_name as 'InsuranceCompanyName',
        '' as 'ClinicName',
        '' as 'ClinicNPI',
        '' as 'ClinicID',
        attending_doctor_first_name as 'AttendingDoctorNameGiven',
        attending_doctor_middle_name as 'AttendingDoctorNameSecondGiven',
        attending_doctor_last_name as 'AttendingDoctorNameFamily',
        attending_doctor_degree as 'AttendingDoctorDegree',
        attending_doctor_id as 'AttendingDoctorNPI',
        '' as 'AttendingDoctorAdministrativeSex',
        '' as 'AttendingDoctorSpecialty',
        '' as 'AttendingDoctorID',
        '' as 'PCPID',
        '' as 'ProcedurePrimaryCPT',
        '' as 'Procedure2CPT',
        '' as 'Procedure3CPT'
FROM hl7app.adt_msg_queue


4/29/2017


select *
from adt_msg_queue
where (sending_facility_id = 'SJRE'
OR sending_facility_id = 'SJR')
AND processing_status = 'r';

select *
from adt_msg_queue
where (sending_facility_id = 'CFPRVFM'
OR sending_facility_id = 'PREV'
OR sending_facility_id = 'WPREVEA')
AND system_timestamp > '201706290000'
AND MRN = '80250598';


select *
from adt_msg_queue amq
INNER JOIN (
    select visit_number, MAX(system_timestamp) as maxTS from adt_msg_queue
    group by visit_number
) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
where (sending_facility_id = 'CFPRVFM'
OR sending_facility_id = 'PREV'
OR sending_facility_id = 'WPREVEA')
AND system_timestamp > '201706290000'
AND msg_type = 'A08'
AND MRN = '64331708';

update adt_msg_queue amq
INNER JOIN (
    select visit_number, MAX(system_timestamp) as maxTS from adt_msg_queue
    group by visit_number
) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
set processing_status = 'r'
where (sending_facility_id = 'CFPRVFM'
OR sending_facility_id = 'PREV'
OR sending_facility_id = 'WPREVEA')
AND system_timestamp > '201706290000'
AND msg_type = 'A08'
AND MRN = '64331708'
INNER JOIN (
    select visit_number, MAX(system_timestamp) as maxTS from adt_msg_queue
    group by visit_number
) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS;
where (sending_facility_id = 'CFPRVFM'
OR sending_facility_id = 'PREV'
OR sending_facility_id = 'WPREVEA')
AND system_timestamp > '201706290000'
AND msg_type = 'A08'
AND MRN = '64331708';



select distinct sending_facility_id
from adt_msg_queue;

ALTER TABLE adt_msg_queue modify COLUMN death_indicator varchar(20);

ALTER TABLE adt_msg_queue modify COLUMN visit_type varchar(20);

set GLOBAL event_scheduler = 1;

drop event hl7_export_records_prevea_1430;

drop event hl7_export_records_cha;
