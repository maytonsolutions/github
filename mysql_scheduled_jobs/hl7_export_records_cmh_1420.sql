delimiter &

CREATE EVENT hl7_export_records_cmh_1420
    ON SCHEDULE
      EVERY 1 day
      STARTS '2018-02-14 14:20:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		    SET processing_status= 'p'
		    WHERE processing_status = 'r'
        AND sending_facility_id = 'CMH'
        AND ((admit_datetime < now() - 10 
                AND location <> 'CHCCLI'
                AND location <> 'CHCHCRXDME')
            OR
            (admit_datetime < now() - INTERVAL 60 DAY
            AND (location = 'CHCCLI' 
                OR  location = 'CHCHCRXDME')));
        
 

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
        privacy_indicator as 'ServiceIndicator01'
	    FROM hl7app.adt_msg_queue
		WHERE processing_status = 'p'
        AND sending_facility_id = 'CMH'
        AND (location <> '1HALL' 
		    AND location <> '2HENSON' 
            AND location <> '3T-ICN' 
            AND location <> '3H-ICN'
            AND location <> '4HH'
            AND location <> '4S'
            AND location <> '5H'
            AND location <> '5HH'
            AND location <> '5S'
            AND location <> '6HALL'
            AND location <> '6H'
            AND location <> '2S-PICU'
            AND location <> 'SC2S'
            AND location <> 'SC3S'
            AND location <> 'FHCINPT'
            AND location <> 'RADOP'
            AND location <> 'NEUP LAB'
            AND location <> 'SCRAD'
            AND location <> 'SCNEUP LAB'
            AND location <> 'SCSLEEP' 
            AND location <> 'CHCHCRXDME'
            AND location <> 'CHCCLI'
            AND location <> 'SCROC'
            AND location <> '2H'
            AND location <> '2HPICUOF' ) "
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
        privacy_indicator as 'ServiceIndicator01'
	    FROM hl7app.adt_msg_queue
		WHERE processing_status = 'p'
        AND sending_facility_id = 'CMH'
        AND (location = '1HALL' 
		    OR location = '2HENSON' 
            OR location = '3T-ICN' 
            OR location = '3H-ICN'
            OR location = '4HH'
            OR location = '4S'
            OR location = '5H'
            OR location = '5HH'
            OR location = '5S'
            OR location = '6HALL'
            OR location = '6H'
            OR location = '2S-PICU'
            OR location = 'SC2S'
            OR location = 'SC3S'
            OR location = 'FHCINPT'
            OR location = 'RADOP'
            OR location = 'NEUP LAB'
            OR location = 'SCRAD'
            OR location = 'SCNEUP LAB'
            OR location = 'SCSLEEP'
            OR location = 'CHCHCRXDME'
            OR location = 'CHCCLI' 
            OR location = 'SCROC'
            OR location = '2H'
            OR location = '2HPICUOF') "
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/CMH_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"'
         LINES TERMINATED BY '\n';"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'd'
		WHERE processing_status = 'p'
        AND sending_facility_id = 'CMH';

      END  &
  
  delimiter ;