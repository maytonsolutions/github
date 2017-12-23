delimiter &

CREATE EVENT hl7_export_records_cha50003_0845
    ON SCHEDULE
      EVERY 1 day
      STARTS '2017-10-18 08:45:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'CHA50003')
        AND msg_type = 'A03'
        AND system_timestamp < now() - 10;

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'CHA50003')
        AND (msg_type = 'A04' or msg_type = 'A08')
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number AS v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'A03'
                AND (customer_id = 'CHA50003')
				AND processing_status= 'p'
            ) AS c
        );
        
        UPDATE LOW_PRIORITY hl7app.adt_msg_queue amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.system_timestamp) as maxTS 
            from adt_msg_queue adt
            where msg_type = 'S14'
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
		SET processing_status= 'p'
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'CHA50003')
        AND amq.msg_type = 'S14'
        AND amq.visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue mq
				WHERE mq.msg_type = 'A03'
                AND mq.processing_status= 'p'
                AND (mq.customer_id = 'CHA50003')
                GROUP by v_number
            ) AS cc
        );



        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'CHA50003')
        AND msg_type = 'A04'
        AND system_timestamp < now() - INTERVAL 1 DAY;
        
        UPDATE LOW_PRIORITY hl7app.adt_msg_queue amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.system_timestamp) as maxTS 
            from adt_msg_queue adt
            where msg_type='S14'
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
		SET processing_status= 'p'
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'CHA50003')
        AND amq.msg_type = 'S14'
        AND amq.visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue mq
				WHERE mq.msg_type = 'A04'
                AND mq.processing_status= 'p'
                AND (mq.customer_id = 'CHA50003')
                GROUP by v_number
            ) AS ccc
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.system_timestamp) as maxTS from adt_msg_queue adt
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
		SET processing_status= 'p'
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'CHA50003' )
        AND amq.msg_type = 'A08'
        AND amq.visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue mq
				WHERE mq.msg_type = 'A04'
                AND mq.processing_status= 'p'
                AND (mq.customer_id = 'CHA50003' )
                GROUP by v_number
            ) AS cccc
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'CHA50003' )
        AND msg_type = 'A08'
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'A04'
                AND processing_status= 'p'
                AND (customer_id = 'CHA50003' )
            ) AS ccccc
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'c'
		WHERE processing_status = 'p'
        AND (customer_id = 'CHA50003' )
        AND msg_type = 'A04'
        AND visit_number in (
            SELECT v_number
            FROM (
			    SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'A08'
                AND processing_status= 'p'
                AND (customer_id = 'CHA50003' )
                ) AS cccccc
        );
        
		UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'CHA50003' )
        AND msg_type = 'S14'
        AND visit_number in (
            SELECT v_number
            FROM (
			    SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'S14'
                AND processing_status= 'p'
                AND (customer_id = 'CHA50003' )
                ) AS ccccccc
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
        'Procedure3CPT' "
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
		WHERE processing_status = 'p'
		AND (msg_type = 'A03' OR msg_type = 'A04' OR msg_type = 'A08')
        AND (location NOT LIKE 'RSM%' 
            AND location NOT LIKE 'PTR%' 
            AND location NOT LIKE 'Hook FB' 
            AND location NOT LIKE 'CRCC PT-N')
		AND (customer_id = 'CHA50003' ) "
         ," UNION ALL "
		,"SELECT  t1.patient_first_name as 'PatientNameGiven',
        t1.patient_middle_name as 'PatientNameSecondGiven',
        t1.patient_last_name as 'PatientNameFamily',
        t1.patient_suffix as 'PatientNameSuffix',
        t1.address1 as 'AddressStreet1',
        t1.address2 as 'AddressStreet2',
        t1.city as 'AddressCity',
        t1.state as 'AddressState',
        t1.zip as 'AddressPostalCode',
        t1.area_code as 'PhoneAreaCityCode',
        t1.local_number as 'PhoneLocalNumber',
        t1.mrn as 'MRN',
        t1.dob as 'DateOfBirth',
        t1.gender as 'AdministrativeSex',
        t1.language as 'PrimaryLanguage',
        t1.race as 'Race',
        t1.ethnic_group as 'EthnicGroup',
        t1.marital as 'MaritalStatus',
        t1.email_address as 'Email',
        t1.visit_type as 'PatientClass',
        t1.sending_facility_name as 'FacilityName',
        '' as 'FacilityNPI',
        t1.sending_facility_id as 'FacilityNumber',
        t1.visit_number as 'VisitNumber',
        t1.admit_datetime as 'AdmitDateTime',
        t1.discharge_datetime as 'DischargeDateTime',
        t1.admit_source as 'AdmitSource',
        t1.discharge_status as 'DischargeStatus',
        t1.location as 'DischargeUnitID',
        '' as 'DischargeUnitName',
        '' as 'MSDRG',
        t1.primary_diagnosis as 'DiagnosisPrimaryICD10',
        t1.secondary_diagnosis as 'Diagnosis2ICD10',
        t1.tertiary_diagnosis as 'Diagnosis3ICD10',
        t1.death_indicator as 'IsDeceased',
        '' as 'ICU',
        '' as 'EDAdmit',
        t1.primary_payer_id as 'InsuranceCompanyID',
        t1.primary_payer_name as 'InsuranceCompanyName',
        '' as 'ClinicName',
        '' as 'ClinicNPI',
        '' as 'ClinicID',
        t2.attending_doctor_first_name as 'AttendingDoctorNameGiven',
        t2.attending_doctor_middle_name as 'AttendingDoctorNameSecondGiven',
        t2.attending_doctor_last_name as 'AttendingDoctorNameFamily',
        t2.attending_doctor_degree as 'AttendingDoctorDegree',
        t2.attending_doctor_id as 'AttendingDoctorNPI',
        '' as 'AttendingDoctorAdministrativeSex',
        '' as 'AttendingDoctorSpecialty',
        '' as 'AttendingDoctorID',
        '' as 'PCPID',
        '' as 'ProcedurePrimaryCPT',
        '' as 'Procedure2CPT',
        '' as 'Procedure3CPT'
		FROM hl7app.adt_msg_queue t1, hl7app.adt_msg_queue t2
		WHERE t1.processing_status = 'p'
		AND t1.visit_number = t2.visit_number
		AND (t1.msg_type = 'A03' OR t1.msg_type = 'A04' OR t1.msg_type = 'A08')
        AND (t1.location LIKE 'RSM%' 
            OR t1.location LIKE 'PTR%' 
            OR t1.location LIKE 'Hook FB' 
            OR t1.location LIKE 'CRCC PT-N')
		AND t2.msg_type = 'S14'
		AND t2.processing_status = 'p'
		AND (t1.customer_id = 'CHA50003') "
         ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/CHA50003_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"';"
        );
        
        

        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'd'
		    WHERE processing_status = 'p'
        AND (customer_id = 'CHA50003' );
        
   END &

delimiter ;     
