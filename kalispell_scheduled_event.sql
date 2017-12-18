delimiter &

CREATE EVENT hl7_export_records_kalispell_0830
    ON SCHEDULE
      EVERY 1 day
      STARTS '2017-08-09 13:30:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue
		SET processing_status= 'p'
		WHERE processing_status = 'r'
        AND (customer_id = 'KALISPELL')
        AND msg_type = 'A03'
        AND system_timestamp < now() - 10;


        SET @sql_text_select =
        CONCAT (
        "SELECT  'Facility ID',
        'Facility Name',
        'Visit Type',
        'Visit Number',
        'Visit Date',
        'MRN',
        'Patient First Name',
        'Patient Last Name',
        'Patient Middle',
        'Patient Suffix',
        'Primary Phone',
        'Secondary Phone',
        'Email Address',
        'Address 1',
        'Address 2',
        'City',
        'State',
        'Zip 5',
        'Zip 4',
        'Gender',
        'Race',
        'Language',
        'Marital',
        'DOB',
        'Attending Dr First Name',
        'Attending Dr Last Name',
        'Attending Dr Middle',
        'Attending Dr Suffix',
        'Attending Dr Degree',
        'Attending Dr ID',
        'Doctor ID Type',
        'Primary Payor ID',
        'Primary Payor Name',
        'Primary Plan Type',
        'Primary Diagnosis',
        'Primary DG Coding System',
        'Primary DG Description',
        'Secondary Diagnosis',
        'Secondary DG Coding System',
        'Secondary DG Description',
        'Tertiary Diagnosis',
        'Tertiary DG Coding System'
        'Tertiary DG Description' "
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
        procedure_description as 'ServiceIndicator01'"
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/PREVEA_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p'
         AND (customer_id = 'KALISPELL');"
        );



        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		    WHERE processing_status = 'p'
        AND (customer_id = 'PREVEA');

      END &

delimiter ;


SET @sql_text_select =
        CONCAT (
        "SELECT  'Facility ID',
        'Facility Name',
        'Visit Type',
        'Visit Number',
        'Visit Date',
        'MRN',
        'Patient First Name',
        'Patient Last Name',
        'Patient Middle',
        'Patient Suffix',
        'Primary Phone',
        'Secondary Phone',
        'Email Address',
        'Address 1',
        'Address 2',
        'City',
        'State',
        'Zip 5',
        'Zip 4',
        'Gender',
        'Race',
        'Language',
        'Marital',
        'DOB',
        'Attending Dr First Name',
        'Attending Dr Last Name',
        'Attending Dr Middle',
        'Attending Dr Suffix',
        'Attending Dr Degree',
        'Attending Dr ID',
        'Doctor ID Type',
        'Primary Payor ID',
        'Primary Payor Name',
        'Primary Plan Type',
        'Primary Diagnosis',
        'Primary DG Coding System',
        'Primary DG Description',
        'Secondary Diagnosis',
        'Secondary DG Coding System',
        'Secondary DG Description',
        'Tertiary Diagnosis',
        'Tertiary DG Coding System'
        'Tertiary DG Description' "
        ," UNION ALL "
		,"SELECT  sending_facility_id as 'Facility ID',
        sending_facility_name as 'Facility Name',
        visit_type as 'Visit Type',
        visit_number as 'Visit Number',
        visit_date as 'Visit Date',
        mrn as 'MRN',
        patient_first_name as 'Patient First Name',
        patient_last_name as 'Patient Last Name',
        patient_middle_name as 'Patient Middle',
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
        procedure_description as 'ServiceIndicator01'"
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/PREVEA_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p'
         AND (customer_id = 'KALISPELL');"
        );


        'Primary Payor ID',
        'Primary Payor Name',
        'Primary Plan Type',
        'Primary Diagnosis',
        'Primary DG Coding System',
        'Primary DG Description',
        'Secondary Diagnosis',
        'Secondary DG Coding System',
        'Secondary DG Description',
        'Tertiary Diagnosis',
        'Tertiary DG Coding System'
        'Tertiary DG Description' "
        ," UNION ALL "
		,"SELECT  sending_facility_id as 'Facility ID',
        sending_facility_name as 'Facility Name',
        visit_type as 'Visit Type',
        visit_number as 'Visit Number',
        visit_date as 'Visit Date',
        mrn as 'MRN',
        patient_first_name as 'Patient First Name',
        patient_last_name as 'Patient Last Name',
        patient_middle_name as 'Patient Middle',
        patient_suffix as 'Patient Suffix',
        primary_phone_number as 'Primary Phone',
        secondary_phone_number as 'Secondary Phone',
        email_address as 'Email Address',
        address1 as 'Address 1',
        address2 as 'Address 2',
        city as 'City',
        state as 'State',
        zip5 as 'Zip 5',
        zip4 as 'Zip 4',
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
        procedure_description as 'ServiceIndicator01'"
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/PREVEA_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue
         WHERE processing_status = 'p'
         AND (customer_id = 'KALISPELL');"
        );



        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue
        SET processing_status= 'c'
		    WHERE processing_status = 'p'
        AND (customer_id = 'PREVEA');

      END &

delimiter ;
