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
        ha1.language as 'PrimaryLanguage',
        ha1.race as 'Race',
        ha1.ethnic_group as 'EthnicGroup',
        ha1.marital as 'MaritalStatus',
        IFNULL(ha2.email_address, '') as 'Email',
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
        '' as 'MSDRG',
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
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/UHS0516_HL7_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|' OPTIONALLY ENCLOSED BY '\"'
         ESCAPED BY '\"' 
         LINES TERMINATED BY '\n'
         FROM hl7app.adt_msg_queue_uhs0516 ha1 
         left outer join hl7app.adt_msg_queue_uhs0516 ha2 
         on ha1.visit_number = ha2.visit_number 
             and ha2.processing_status = 'd' 
             and ha2.msg_type = 'A08'
		     where ha1.processing_status = 'd'
             AND ha1.msg_type = 'A03'
             AND ha1.customer_id = 'UHS0516'
             and (ha1.location = 'B301'
                 or ha1.location = 'B032'
                 or ha1.location = 'B076'
                 or ha1.location = 'B145'
                 or ha1.location = 'B160'
                 or ha1.location = 'B188'
                 or ha1.location = 'B218'
                 or ha1.location = 'B496'
                 or ha1.location = 'B755'
                 or ha1.location = 'H260'
                 or ha1.location = 'H405'
                 or ha1.location = 'H442'
                 or ha1.location = 'H805'
                 or ha1.location = 'K130'
                 or ha1.location = 'M708'
                 or ha1.location = 'M723'
                 or ha1.location = 'M907'
                 or ha1.location = 'M913'
                 or ha1.location = 'N600'
                 or ha1.location = 'P486'
                 or ha1.location = 'P557'
                 or ha1.location = 'P588'
                 )
             AND ha1.system_timestamp > '20180524000000'  ;"
        );
        
        

        PREPARE s1 FROM @sql_text_select;
        EXECUTE s1;
        DROP PREPARE s1;
