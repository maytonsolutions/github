delimiter &

CREATE EVENT hl7_export_records_OSU0523_OP_1535
    ON SCHEDULE
      EVERY 1 day
      STARTS '2018-01-18 20:35:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO


BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_osu0523
		    SET processing_status= 'p'
		    WHERE processing_status = 'r'
        AND customer_id = 'OSU0523'
        AND msg_type = 'A03'
        AND visit_type <> 'I'
        AND system_timestamp < now() - INTERVAL 1 DAY;
        
        UPDATE low_priority hl7app.adt_msg_queue_osu0523 amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.msg_send_timestamp) as maxTS 
            from adt_msg_queue_osu0523 adt
            where adt.msg_type = 'A13'
            and processing_status = 'r'
            and customer_id = 'OSU0523'
            group by visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.msg_send_timestamp <= maxTS
		set amq.processing_status = 'f'
		WHERE amq.processing_status = 'p'
        AND amq.customer_id = 'OSU0523'
        AND amq.msg_type = 'A03'
        AND visit_type <> 'I';
        
        UPDATE low_priority hl7app.adt_msg_queue_osu0523 
        set processing_status = 'c'
        where msg_type = 'A13'
        and customer_id = 'OSU0523'
        and processing_status = 'r'
        and visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue_osu0523 mq
				WHERE mq.msg_type = 'A03'
                AND (mq.processing_status= 'p' or mq.processing_status= 'f')
                AND (mq.customer_id = 'OSU0523')
                AND visit_type <> 'I'
                GROUP by v_number
            ) AS m
        );

		UPDATE LOW_PRIORITY hl7app.adt_msg_queue_osu0523 amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.system_timestamp) as maxTS 
            from adt_msg_queue_osu0523 adt
            where customer_id = 'OSU0523'
            and msg_type = 'A08'
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
		SET processing_status= 'p'
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'OSU0523')
        AND amq.msg_type = 'A08'
        AND amq.visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue_osu0523 mq
				WHERE mq.msg_type = 'A03'
                AND mq.processing_status= 'p'
                AND (mq.customer_id = 'OSU0523')
                GROUP by v_number
            ) AS c
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_osu0523
        SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'OSU0523')
        AND msg_type = 'A08'
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue_osu0523
                WHERE msg_type = 'A03'
                AND processing_status= 'p'
                AND (customer_id = 'OSU0523')
            ) AS d
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
        'ServiceIndicator01' "
        ," UNION ALL "
		,"SELECT  ha1.patient_first_name as 'PatientNameGiven',
        ha1.patient_middle_name as 'PatientNameSecondGiven',
        ha1.patient_last_name as 'PatientNameFamily',
        ha1.patient_suffix as 'PatientNameSuffix',
        ha1.address1 as 'AddressStreet1',
        ha1.address2 as 'AddressStreet2',
        ha1.city as 'AddressCity',
        ha1.state as 'AddressState',
        ha1.zip as 'AddressPostalCode',
        ha1.area_code as 'PhoneAreaCityCode',
        ha1.local_number as 'PhoneLocalNumber',
        ha1.mrn as 'MRN',
        ha1.dob as 'DateOfBirth',
        ha1.gender as 'AdministrativeSex',
        IFNULL(ha2.language,'') as 'PrimaryLanguage',
        ha1.race as 'Race',
        IFNULL(ha2.ethnic_group,'') as 'EthnicGroup',
        ha1.marital as 'MaritalStatus',
        ha1.email_address as 'Email',
        ha1.visit_type as 'PatientClass',
        ha1.sending_facility_name as 'FacilityName',
        '' as 'FacilityNPI',
        ha1.sending_facility_id as 'FacilityNumber',
        ha1.visit_number as 'VisitNumber',
        ha1.admit_datetime as 'AdmitDateTime',
        ha1.discharge_datetime as 'DischargeDateTime',
        ha1.admit_source as 'AdmitSource',
        ha1.discharge_status as 'DischargeStatus',
        ha1.location as 'DischargeUnitID',
        '' as 'DischargeUnitName',
        IFNULL(ha2.ms_drg, '') as 'MSDRG',
        ha1.primary_diagnosis as 'DiagnosisPrimaryICD10',
        ha1.secondary_diagnosis as 'Diagnosis2ICD10',
        ha1.tertiary_diagnosis as 'Diagnosis3ICD10',
        ha1.death_indicator as 'IsDeceased',
        '' as 'ICU',
        '' as 'EDAdmit',
        ha1.primary_payer_id as 'InsuranceCompanyID',
        ha1.primary_payer_name as 'InsuranceCompanyName',
        '' as 'ClinicName',
        '' as 'ClinicNPI',
        '' as 'ClinicID',
        ha1.attending_doctor_first_name as 'AttendingDoctorNameGiven',
        ha1.attending_doctor_middle_name as 'AttendingDoctorNameSecondGiven',
        ha1.attending_doctor_last_name as 'AttendingDoctorNameFamily',
        ha1.attending_doctor_degree as 'AttendingDoctorDegree',
        ha1.attending_doctor_id as 'AttendingDoctorNPI',
        '' as 'AttendingDoctorAdministrativeSex',
        '' as 'AttendingDoctorSpecialty',
        '' as 'AttendingDoctorID',
        '' as 'PCPID',
        '' as 'ProcedurePrimaryCPT',
        '' as 'Procedure2CPT',
        '' as 'Procedure3CPT',
        '' as 'ServiceIndicator01'"
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/OSU0523_HL7_OP"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"'
		 LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue_osu0523 ha1 
         left outer join hl7app.adt_msg_queue_osu0523 ha2 
         on ha1.visit_number = ha2.visit_number 
		 and ha2.processing_status = 'p' 
         and ha2.msg_type = 'A08'
		 where ha1.processing_status = 'p'
         AND ha1.msg_type = 'A03'
         AND ha1.customer_id = 'OSU0523';"
        );
        
        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;
        
        UPDATE hl7app.adt_msg_queue_osu0523
        SET processing_status= 'd'
		WHERE processing_status = 'p'
        AND visit_type <> 'I'
        AND customer_id = 'OSU0523';
        
      END &
delimiter ; 