delimiter &

CREATE EVENT hl7_export_records_hmh0530_IP_0610
    ON SCHEDULE
      EVERY 1 day
      STARTS '2018-02-12 11:10:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN

        UPDATE LOW_PRIORITY hl7app.adt_msg_queue_hmh0530
		    SET processing_status= 'p'
		    WHERE processing_status = 'r'
        AND customer_id = 'HMH0530'
        AND msg_type = 'A03'
        AND (visit_type = 'I' 
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
        AND system_timestamp < now() - 10; 


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
        'I' as 'PatientClass',
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
        clinic_name as 'ClinicName',
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
	    FROM hl7app.adt_msg_queue_hmh0530
		WHERE processing_status = 'p'
        AND (visit_type = 'I' 
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
        AND customer_id = 'HMH0530' "
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/HMH0530_HL7_IP_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"'
         LINES TERMINATED BY '\n';"
        );


        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue_hmh0530
        SET processing_status= 'd'
		WHERE processing_status = 'p'
        AND (visit_type = 'I' 
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
        AND customer_id = 'HMH0530';
        
        END &
delimiter ;       