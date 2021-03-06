delimiter &

CREATE EVENT hl7_export_records_kalispell_0830
    ON SCHEDULE
      EVERY 1 hour
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
        gender as 'Gender',
        race as 'Race',
        language as 'Language',
        marital as 'Marital',
        dob as 'DOB',
        attending_doctor_first_name as 'Attending Dr First Name',
		attending_doctor_last_name as 'Attending Dr Last Name',
        attending_doctor_middle_name as 'Attending Dr Middle',
		attending_doctor_degree as 'Attending Dr Degree',
        attending_doctor_id as 'Attending Dr ID',
        attending_doctor_id_type as 'Doctor ID Type',
        primary_payer_id as 'Primary Payor ID',
        primary_payer_name as 'Primary Payor Name',
        primary_plan_type as 'Primary Plan Type',
        primary_diagnosis as 'Primary Diagnosis',
        primary_diag_coding_system as 'Primary DG Coding System',
        primary_diag_description as 'Primary DG Description',
        secondary_diagnosis as 'Secondary Diagnosis',
        secondary_diag_coding_system as 'Secondary DG Coding System',
        secondary_diag_description as 'Secondary DG Description',
        tertiary_diagnosis as 'Tertiary Diagnosis',
        tertiary_diag_coding_system as 'Tertiary DG Coding System',
        tertiary_diag_description as 'Tertiary DG Description'"
        ," into outfile 'C:/ProgramData/MySQL/MySQL Server 5.7/Uploads/KALISPELL_"
         , DATE_FORMAT( NOW(), '%Y%m%d%H%i%S%f')
         , " ' FIELDS TERMINATED BY '|'
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
        AND (customer_id = 'KALISPELL');

      END &

delimiter ;
