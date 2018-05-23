CREATE TABLE `adt_msg_queue_ohsu` (
  `msg_controlid` varchar(80) NOT NULL,
  `sending_facility_id` varchar(180) NOT NULL,
  `msg_send_timestamp` varchar(26) NOT NULL,
  `msg_type` varchar(3) NOT NULL,
  `system_timestamp` varchar(50) NOT NULL,
  `system_unique_id` varchar(80) NOT NULL,
  `customer_id` varchar(180) DEFAULT NULL,
  `customer_name` varchar(180) DEFAULT NULL,
  `sending_facility_name` varchar(180) DEFAULT NULL,
  `patient_external_id` varchar(80) DEFAULT NULL,
  `patient_internal_id` varchar(80) DEFAULT NULL,
  `mrn` varchar(80) DEFAULT NULL,
  `patient_account_number` varchar(20) DEFAULT NULL,
  `patient_last_name` varchar(48) DEFAULT NULL,
  `patient_first_name` varchar(48) DEFAULT NULL,
  `patient_middle_name` varchar(48) DEFAULT NULL,
  `patient_suffix` varchar(48) DEFAULT NULL,
  `primary_phone_number` varchar(40) DEFAULT NULL,
  `area_code` varchar(3) DEFAULT NULL,
  `local_number` varchar(7) DEFAULT NULL,
  `secondary_phone_number` varchar(40) DEFAULT NULL,
  `email_address` varchar(180) DEFAULT NULL,
  `address1` varchar(106) DEFAULT NULL,
  `address2` varchar(106) DEFAULT NULL,
  `city` varchar(40) DEFAULT NULL,
  `state` varchar(40) DEFAULT NULL,
  `zip` varchar(10) DEFAULT NULL,
  `zip5` varchar(5) DEFAULT NULL,
  `zip4` varchar(4) DEFAULT NULL,
  `gender` varchar(20) DEFAULT NULL,
  `race` varchar(80) DEFAULT NULL,
  `language` varchar(60) DEFAULT NULL,
  `marital` varchar(80) DEFAULT NULL,
  `dob` varchar(26) DEFAULT NULL,
  `visit_number` varchar(20) DEFAULT NULL,
  `visit_type` varchar(20) DEFAULT NULL,
  `visit_date` varchar(26) DEFAULT NULL,
  `attending_doctor_id` varchar(60) DEFAULT NULL,
  `attending_doctor_id_type` varchar(60) DEFAULT NULL,
  `attending_doctor_last_name` varchar(60) DEFAULT NULL,
  `attending_doctor_first_name` varchar(60) DEFAULT NULL,
  `attending_doctor_middle_name` varchar(45) DEFAULT NULL,
  `attending_doctor_suffix` varchar(45) DEFAULT NULL,
  `attending_doctor_degree` varchar(45) DEFAULT NULL,
  `primary_payer_id` varchar(60) DEFAULT NULL,
  `primary_payer_name` varchar(130) DEFAULT NULL,
  `primary_plan_type` varchar(60) DEFAULT NULL,
  `primary_diagnosis` varchar(60) DEFAULT NULL,
  `primary_diag_coding_system` varchar(60) DEFAULT NULL,
  `primary_diag_description` varchar(256) DEFAULT NULL,
  `secondary_diagnosis` varchar(60) DEFAULT NULL,
  `secondary_diag_coding_system` varchar(60) DEFAULT NULL,
  `secondary_diag_description` varchar(256) DEFAULT NULL,
  `tertiary_diagnosis` varchar(60) DEFAULT NULL,
  `tertiary_diag_coding_system` varchar(60) DEFAULT NULL,
  `tertiary_diag_description` varchar(256) DEFAULT NULL,
  `location` varchar(180) DEFAULT NULL,
  `admit_datetime` varchar(26) DEFAULT NULL,
  `discharge_datetime` varchar(26) DEFAULT NULL,
  `discharge_status` varchar(30) DEFAULT NULL,
  `emergency_severity_index` varchar(20) DEFAULT NULL,
  `ethnic_group` varchar(60) DEFAULT NULL,
  `death_indicator` varchar(20) DEFAULT NULL,
  `processing_status` varchar(1) DEFAULT NULL,
  `msg` mediumtext,
  `admit_source` varchar(60) DEFAULT NULL,
  `privacy_indicator` varchar(20) DEFAULT NULL,
  `ms_drg` varchar(60) DEFAULT NULL,
  `msdrg_coding_system` varchar(60) DEFAULT NULL,
  `msdrg_alt_coding_system` varchar(60) DEFAULT NULL,
  `procedure_description` varchar(100) DEFAULT NULL,
  `clinic_name` varchar(180) DEFAULT NULL,
  `service_indicator01` varchar(60) DEFAULT NULL,
  `service_indicator02` varchar(60) DEFAULT NULL,
  `pass_through01` varchar(60) DEFAULT NULL,
  PRIMARY KEY (`msg_controlid`,`sending_facility_id`,`msg_send_timestamp`,`msg_type`,`system_timestamp`,`system_unique_id`),
  KEY `idx_adt_msg_queue_customer_id` (`customer_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='This is the table to capture incoming admission, registration ADT messages';
