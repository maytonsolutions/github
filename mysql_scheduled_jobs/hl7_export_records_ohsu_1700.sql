delimiter &

CREATE EVENT hl7_export_records_ohsu_1700
    ON SCHEDULE
      EVERY 1 day
      STARTS '2018-05-16 21:50:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN


        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_ohsu
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'OHSU')
        AND msg_type = 'A03'
        AND system_timestamp < now() - 10;

		UPDATE LOW_PRIORITY hl7app.adt_msg_queue_ohsu
		SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'OHSU')
        AND (msg_type = 'A04' or msg_type = 'A08')
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number AS v_number
                FROM hl7app.adt_msg_queue_ohsu
                WHERE msg_type = 'A03'
				AND processing_status= 'p'
            ) AS c
        );


        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_ohsu
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'OHSU')
        AND msg_type = 'A04'
        AND system_timestamp < now();


        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_ohsu amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.system_timestamp) as maxTS from adt_msg_queue_ohsu adt
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
		SET processing_status= 'p'
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'OHSU')
        AND amq.msg_type = 'A08'
        AND amq.visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue_ohsu mq
				WHERE mq.msg_type = 'A04'
                AND mq.processing_status= 'p'
                AND (mq.customer_id = 'OHSU')
                GROUP by v_number
            ) AS c
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_ohsu
        SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'OHSU')
        AND msg_type = 'A08'
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue_ohsu
                WHERE msg_type = 'A04'
                AND processing_status= 'p'
                AND (customer_id = 'OHSU')
            ) AS c
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_ohsu
		SET processing_status= 'c'
		WHERE processing_status = 'p'
        AND (customer_id = 'OHSU')
        AND msg_type = 'A04'
        AND visit_number in (
            SELECT v_number
            FROM (
			    SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue_ohsu
                WHERE msg_type = 'A08'
                AND processing_status= 'p'
                AND (customer_id = 'OHSU')
                ) AS c
        );



        SET @sql_text_select =
        CONCAT (
        "SELECT  'PatientNameGiven',
        'PatientNameSecondGiven',
        'PatientNameFamily',
        'PatientNameSuffix',
        'AddressStreet1',
        'AddressStreet2',
        'AddressCity',
        'AddressState',
        'AddressPostalCode',
        'PhoneAreaCityCode',
        'PhoneLocalNumber',
        'MRN',
        'DateOfBirth',
        'AdministrativeSex',
        'PrimaryLanguage',
        'Race',
        'EthnicGroup',
        'MaritalStatus',
        'Email',
        'PatientClass',
        'FacilityName',
        'FacilityNPI',
        'FacilityNumber',
        'VisitNumber',
        'AdmitDateTime',
        'DischargeDateTime',
        'AdmitSource',
        'DischargeStatus',
        'DischargeUnitID',
        'DischargeUnitName',
        'MSDRG',
        'DiagnosisPrimaryICD10',
        'Diagnosis2ICD10',
        'Diagnosis3ICD10',
        'IsDeceased',
        'ICU',
        'EDAdmit',
        'InsuranceCompanyID',
        'InsuranceCompanyName',
        'ClinicName',
        'ClinicNPI',
        'ClinicID',
        'AttendingDoctorNameGiven',
        'AttendingDoctorNameSecondGiven',
        'AttendingDoctorNameFamily',
        'AttendingDoctorDegree',
        'AttendingDoctorNPI',
        'AttendingDoctorAdministrativeSex',
        'AttendingDoctorSpecialty',
        'AttendingDoctorID',
		'PCPID',
        'ProcedurePrimaryCPT',
        'Procedure2CPT',
        'Procedure3CPT', 
        'ServiceIndicator01',
        'ServiceIndicator02',
        'ServiceIndicator03',
        'PassThrough02' "
        ," UNION ALL "
		,"SELECT  patient_first_name as 'PatientNameGiven',
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
        sending_facility_id as 'FacilityNumber',
        visit_number as 'VisitNumber',
        admit_datetime as 'AdmitDateTime',
        discharge_datetime as 'DischargeDateTime',
        admit_source as 'AdmitSource',
        discharge_status as 'DischargeStatus',
        location as 'DischargeUnitID',
        '' as 'DischargeUnitName',
        IFNULL(ms_drg,'') as 'MSDRG',
        primary_diagnosis as 'DiagnosisPrimaryICD10',
        secondary_diagnosis as 'Diagnosis2ICD10',
        tertiary_diagnosis as 'Diagnosis3ICD10',
        death_indicator as 'IsDeceased',
        '' as 'ICU',
        '' as 'EDAdmit',
        primary_payer_id as 'InsuranceCompanyID',
        primary_payer_name as 'InsuranceCompanyName',
        '' as 'ClinicName',
        IFNULL(clinic_name,'') as 'ClinicNPI',
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
        '' as 'Procedure3CPT',
        IFNULL(service_indicator01, '') as 'ServiceIndicator01',
        IFNULL(service_indicator02, '') as 'ServiceIndicator02',
        IFNULL(service_indicator03, '') as 'ServiceIndicator03',
        IFNULL(pass_through01, '') as 'PassThrough01'"
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/OHSU_HL7_OP"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue_ohsu
         WHERE processing_status = 'p'
         AND customer_id = 'OHSU';"
        );
        
        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;
        
        UPDATE hl7app.adt_msg_queue_ohsu
        SET processing_status= 'd'
		WHERE processing_status = 'p'
        AND customer_id = 'OHSU';
        
   END &
delimiter ;   