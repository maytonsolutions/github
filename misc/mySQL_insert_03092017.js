var dbConn;

try {
	var dateString = DateUtil.getCurrentDate('yyyyMMddHHmmss');
	var uuid = UUIDGenerator.getUUID();
	var msg = (new String(connectorMessage.getRawData())).replace(/'/g, "\\'");

	dbConn = DatabaseConnectionFactory.createDatabaseConnection('com.mysql.jdbc.Driver','jdbc:mysql://localhost:3306/hl7app','mirth','mirth');



	var result = dbConn.executeUpdate("INSERT INTO adt_msg_queue (msg_controlid, sending_facility, msg_send_timestamp, msg_type, system_timestamp, system_unique_id, " +
	" patient_external_id, patient_internal_id, patient_last_name , patient_first_name, patient_middle_name, patient_account_number, visit_number, attending_doctor_id, attending_doctor_last_name, attending_doctor_first_name, " +
	" attending_doctor_middle_name, admit_timestamp, discharge_timestamp, processing_status, msg)" +
	"  VALUES ('"+$('msg_controlid')+"', '"+$('send_facility')+"', '"+$('msg_send_timestamp')+"', '"+$('msg_type')+"', '"+dateString+"', '"+uuid+"' , '"+$('patient_external_id')+"', '" +
	$('patient_internal_id')+"', '"+$('patient_last_name')+"', '"+$('patient_first_name')+"', '"+$('patient_middle_name')+"' , '"+$('patient_account_number')+"' , '"+$('visit_number')+"' , '"+$('attending_doctor_id')+"', '" +
	$('attending_doctor_last_name') +  "', '"+$('attending_doctor_first_name')+"','"+$('attending_doctor_middle_name')+"', '"+$('admit_timestamp')+"', '"+$('discharge_timestamp')+"', '"+$('processing_status')+"', '"+msg+"')");



}catch(e){
    logger.error($('Channel Name') + $('send_facility') + $('msg_controlid') +  dateString + e );

    throw (e);
}finally {
	if (dbConn) {
		dbConn.close();
	}
}
