channelMap.put('msg_controlid',msg['MSH']['MSH.10']['MSH.10.1'].toString());

if(msg['MSH']['MSH.4']['MSH.4.1'].toString() != null && msg['MSH']['MSH.4']['MSH.4.1'].toString() != ''){
    channelMap.put('send_facility',msg['MSH']['MSH.4']['MSH.4.1'].toString());
} else if (msg['PV1']['PV1.3']['PV1.3.7'].toString() != null && msg['PV1']['PV1.3']['PV1.3.7'].toString() != '') {
    channelMap.put('send_facility',msg['PV1']['PV1.3']['PV1.3.7'].toString());
}else if (msg['PV1']['PV1.39']['PV1.39.1'].toString() != null && msg['PV1']['PV1.39']['PV1.39.1'].toString() != ''){
    channelMap.put('send_facility',msg['PV1']['PV1.39']['PV1.39.1'].toString() );
}else if (msg['PV2']['PV2.23']['PV2.23.1'].toString()!= null && msg['PV2']['PV2.23']['PV2.23.1'].toString() != ''){
	channelMap.put('send_facility', msg['PV2']['PV2.23']['PV2.23.1'].toString());;
}

channelMap.put('msg_send_timestamp',msg['MSH']['MSH.7']['MSH.7.1'].toString());
channelMap.put('msg_type', msg['MSH']['MSH.9']['MSH.9.2'].toString());
channelMap.put('patient_external_id', msg['PID']['PID.2']['PID.2.1'].toString());
channelMap.put('patient_internal_id', msg['PID']['PID.3']['PID.3.1'].toString());
channelMap.put('patient_last_name', (msg['PID']['PID.5'][0]['PID.5.1'].toString()).replace(/'/g, "\\'"));
channelMap.put('patient_first_name', msg['PID']['PID.5'][0]['PID.5.2'].toString());
channelMap.put('patient_middle_name', msg['PID']['PID.5'][0]['PID.5.3'].toString());
channelMap.put('patient_account_number', msg['PID']['PID.18']['PID.18.1'].toString());
channelMap.put('visit_number', msg['PV1']['PV1.19']['PV1.19.1'].toString());
channelMap.put('attending_doctor_id', msg['PV1']['PV1.7'][0]['PV1.7.1'].toString());
channelMap.put('attending_doctor_last_name',  (msg['PV1']['PV1.7'][0]['PV1.7.2'].toString()).replace(/'/g, "\\'"));
channelMap.put('attending_doctor_first_name',  msg['PV1']['PV1.7'][0]['PV1.7.3'].toString());
channelMap.put('attending_doctor_middle_name',  msg['PV1']['PV1.7'][0]['PV1.7.4'].toString());
channelMap.put('admit_timestamp',  msg['PV1']['PV1.44']['PV1.44.1'].toString());
channelMap.put('discharge_timestamp',  msg['PV1']['PV1.45']['PV1.45.1'].toString());
channelMap.put('processing_status',  'n');
