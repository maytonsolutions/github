delimiter &

CREATE EVENT hl7_export_records_menifee_0400
    ON SCHEDULE
      EVERY 1 day
      STARTS '2018-03-16 09:0:00'
    COMMENT 'pick up every new records that are more than 10 seconds old'
    DO

BEGIN


	UPDATE LOW_PRIORITY hl7app.adt_msg_queue_menifee amq         
	INNER JOIN (             
		select adt.visit_number, MAX(adt.msg_send_timestamp) as maxTS from adt_msg_queue_menifee adt             
		group by adt.visit_number         
	) ms on amq.visit_number = ms.visit_number AND amq.msg_send_timestamp = maxTS   
	SET processing_status= 'p'   
	WHERE amq.processing_status = 'r'         
	AND amq.ms_drg <> ''         
	AND (amq.customer_id = 'MENIFEE')         
	AND amq.msg_type = 'A08';
	
	UPDATE hl7app.adt_msg_queue_menifee         
    SET processing_status= 'd'       
    WHERE processing_status = 'p'         
    AND (customer_id = 'MENIFEE');	

	
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
		ms_drg as 'MSDRG',         
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
		'' as 'ServiceIndicator01'"         
		," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/MENIFEE_HL7_IP_DRG"        
        , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"'
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue_menifee
         WHERE processing_status = 'p'
         AND (customer_id = 'MENIFEE');"
        );
        
        

        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;

        UPDATE hl7app.adt_msg_queue_menifee
        SET processing_status= 'd'
		    WHERE processing_status = 'p'
        AND (customer_id = 'MENIFEE');  


      END &

delimiter ;
