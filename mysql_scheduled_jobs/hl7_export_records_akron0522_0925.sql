delimiter &

CREATE EVENT hl7_export_records_akron0522_0925
    ON SCHEDULE
      EVERY 1 day
      STARTS '2018-05-29 14:25:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'AKRON0522')
        AND msg_type = 'A03'
        AND system_timestamp < now() - 10;

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'AKRON0522')
        AND (msg_type = 'A04' or msg_type = 'A08')
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number AS v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'A03'
				AND processing_status= 'p'
            ) AS c
        );


        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'AKRON0522')
        AND msg_type = 'A04'
        AND system_timestamp < now() - INTERVAL 1 DAY;


        UPDATE LOW_PRIORITY hl7app.adt_msg_queue amq
        INNER JOIN (
            select adt.visit_number, MAX(adt.system_timestamp) as maxTS from adt_msg_queue adt
            group by adt.visit_number
        ) ms on amq.visit_number = ms.visit_number AND amq.system_timestamp = maxTS
		SET processing_status= 'p'
		WHERE amq.processing_status = 'r'
        AND (amq.customer_id = 'AKRON0522')
        AND amq.msg_type = 'A08'
        AND amq.visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct mq.visit_number AS v_number
                FROM hl7app.adt_msg_queue mq
				WHERE mq.msg_type = 'A04'
                AND mq.processing_status= 'p'
                AND (mq.customer_id = 'AKRON0522')
                GROUP by v_number
            ) AS c
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
        SET processing_status= 'c'
		WHERE processing_status = 'r'
        AND (customer_id = 'AKRON0522')
        AND msg_type = 'A08'
        AND visit_number in (
            SELECT v_number
            FROM (
                SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'A04'
                AND processing_status= 'p'
                AND (customer_id = 'AKRON0522')
            ) AS c
        );

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'c'
		WHERE processing_status = 'p'
        AND (customer_id = 'AKRON0522')
        AND msg_type = 'A04'
        AND visit_number in (
            SELECT v_number
            FROM (
			    SELECT distinct visit_number as v_number
                FROM hl7app.adt_msg_queue
                WHERE msg_type = 'A08'
                AND processing_status= 'p'
                AND (customer_id = 'AKRON0522')
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
        'ServiceIndicator01' "
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
        '' as 'Procedure3CPT',
        '' as 'ServiceIndicator01'
        FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p'
         AND (location <> 'AUD'
		     AND location <> 'AUH'
             AND location <> 'XAD'
             AND location <> 'AUM'
             AND location <> 'AUW'
             AND location <> 'OCC'
             AND location <> 'OHD'
             AND location <> 'XOT'
             AND location <> 'OMA'
             AND location <> 'OCM'
             AND location <> 'OCN'
             AND location <> 'O2N'
             AND location <> 'OTR'
             AND location <> 'PHY'
             AND location <> 'PTH'
             AND location <> 'XPT'
             AND location <> 'PMA'
             AND location <> 'PTM'
             AND location <> 'PTN'
             AND location <> 'P2N'
             AND location <> 'PTW'
             AND location <> 'SPR'
             AND location <> 'PTL'
             AND location <> 'SRM'
             AND location <> 'SPE'
             AND location <> 'STH'
             AND location <> 'XSP'
             AND location <> 'SMA'
             AND location <> 'SME'
             AND location <> 'SPM'
             AND location <> 'STN'
             AND location <> 'S2N'
             AND location <> 'SPW'
             AND location <> 'CLB'
             AND location <> 'CHO'
             AND location <> 'LLB'
             AND location <> 'XLB'
             AND location <> 'LAW'
             AND location <> 'MRI'
             AND location <> 'CAT'
             AND location <> 'ULT'
             AND location <> 'NUC'
             AND location <> 'RAD'
             AND location <> 'MR2'
             AND location <> 'MR3'
             AND location <> 'HRD'
             AND location <> 'HUC'
             AND location <> 'MUS'
             AND location <> 'MCT'
             AND location <> 'MMR'
             AND location <> 'MNM'
             AND location <> 'XRD'
             AND location <> 'RWA'
             AND location <> 'ULW')       
         AND (customer_id = 'AKRON0522')  "        
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
        '' as 'AttendingDoctorNameGiven',
        '' as 'AttendingDoctorNameSecondGiven',
        '' as 'AttendingDoctorNameFamily',
        '' as 'AttendingDoctorDegree',
        '' as 'AttendingDoctorNPI',
        '' as 'AttendingDoctorAdministrativeSex',
        '' as 'AttendingDoctorSpecialty',
        '' as 'AttendingDoctorID',
        '' as 'PCPID',
        '' as 'ProcedurePrimaryCPT',
        '' as 'Procedure2CPT',
        '' as 'Procedure3CPT',
        '' as 'ServiceIndicator01'
		FROM hl7app.adt_msg_queue
        WHERE processing_status = 'p'
		AND (location = 'AUD'
		     OR location = 'AUH'
             OR location = 'XAD'
             OR location = 'AUM'
             OR location = 'AUW'
             OR location = 'OCC'
             OR location = 'OHD'
             OR location = 'XOT'
             OR location = 'OMA'
             OR location = 'OCM'
             OR location = 'OCN'
             OR location = 'O2N'
             OR location = 'OTR'
             OR location = 'PHY'
             OR location = 'PTH'
             OR location = 'XPT'
             OR location = 'PMA'
             OR location = 'PTM'
             OR location = 'PTN'
             OR location = 'P2N'
             OR location = 'PTW'
             OR location = 'SPR'
             OR location = 'PTL'
             OR location = 'SRM'
             OR location = 'SPE'
             OR location = 'STH'
             OR location = 'XSP'
             OR location = 'SMA'
             OR location = 'SME'
             OR location = 'SPM'
             OR location = 'STN'
             OR location = 'S2N'
             OR location = 'SPW'
             OR location = 'CLB'
             OR location = 'CHO'
             OR location = 'LLB'
             OR location = 'XLB'
             OR location = 'LAW'
             OR location = 'MRI'
             OR location = 'CAT'
             OR location = 'ULT'
             OR location = 'NUC'
             OR location = 'RAD'
             OR location = 'MR2'
             OR location = 'MR3'
             OR location = 'HRD'
             OR location = 'HUC'
             OR location = 'MUS'
             OR location = 'MCT'
             OR location = 'MMR'
             OR location = 'MNM'
             OR location = 'XRD'
             OR location = 'RWA'
             OR location = 'ULW') 
         AND (customer_id = 'AKRON0522') "
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/AKRON0522_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"' 
         LINES TERMINATED BY '\n' ;"
        );
        
        

        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'd'
		    WHERE processing_status = 'p'
        AND (customer_id = 'AKRON0522');
        
        SELECT '1' INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/AKRON0522.OK';

      END &

delimiter ;