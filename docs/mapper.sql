out.Segment1.PATIENT_TYPE="CONFIRMED";
	out.Segment1.EXTERNAL_ID = in.NewStatement.UNID.#PCDATA;

	MapSTREET1Tostring(in.NewStatement.STREET1, out.Segment1.LTCF);
	out.Segment1.FULL_NAME = in.NewStatement.FIRST_NAME.#PCDATA + " "  + in.NewStatement.LAST_NAME.#PCDATA;
	out.Segment1.NAME = in.NewStatement.FIRST_NAME.#PCDATA + " "  + in.NewStatement.LAST_NAME.#PCDATA;
	out.Segment1.ADDRESS = in.NewStatement.STREET1.#PCDATA;
	out.Segment1.ADDRESS_CITY = in.NewStatement.CITY.#PCDATA;
	out.Segment1.ADDRESS_COUNTY = in.NewStatement.CITY.#PCDATA;
	out.Segment1.AGE = StrFieldExtract(in.NewStatement.AGE_YEARS.#PCDATA,".",1);
	out.Segment1.TEST_TYPE = in.NewStatement.TEST.#PCDATA;
	out.Segment1.CDMS_CREATE_DATE = in.NewStatement.CREATE_DATE.#PCDATA;
	out.Segment1.CASE_IMPORT_DATE = in.NewStatement.CREATE_DATE.#PCDATA;
	out.Segment1.ADDRESS_STATE = in.NewStatement.STATE.#PCDATA;
	out.Segment1.ADDRESS_ZIP = in.NewStatement.POSTAL_CODE.#PCDATA;
	out.Segment1.FIRST_NAME = in.NewStatement.FIRST_NAME.#PCDATA;
	out.Segment1.LAST_NAME = in.NewStatement.LAST_NAME.#PCDATA;
	out.Segment1.DOB = in.NewStatement.BIRTH_DATE.#PCDATA;
	out.Segment1.TEST_TYPE = in.NewStatement.TEST.#PCDATA;
	out.Segment1.RACE = in.NewStatement.RACE.#PCDATA;
	out.Segment1.LAB_RESULT = in.NewStatement.RESULT.#PCDATA;
	out.Segment1.GENDER = in.NewStatement.GENDER.#PCDATA;
	out.Segment1.ANALYSIS_DATE = in.NewStatement.RESULT_DATE.#PCDATA;
	out.Segment1.LAB_FACILITY = StrFieldExtract(in.NewStatement.FACILITY.#PCDATA,",",1);
	out.Segment1.ORDERING_FACILITY = StrFieldExtract(in.NewStatement.ORDER_FACILITY.#PCDATA,",",1);
	out.Segment1.SPECIMEN_COLLECTION_DATE = in.NewStatement.SPECIMEN_DATE.#PCDATA;
	out.Segment1.ORDERING_PROVIDER = in.NewStatement.ORDER_PROVIDER.#PCDATA;
	out.Segment1.CDMS_ID = in.NewStatement.CASE_ID.#PCDATA;
	if (!isempty(in.NewStatement.MOBILE_PHONE)){
		out.Segment1.PHONE_HOME = StrRemoveChars(in.NewStatement.MOBILE_PHONE.#PCDATA,"()- ");
}
	else out.Segment1.PHONE_HOME = StrRemoveChars(in.NewStatement.HOME_PHONE.#PCDATA,"()- ");
	if (!isempty(in.NewStatement.HISPANIC.#PCDATA)){
		if (in.NewStatement.HISPANIC.#PCDATA=="Yes"){
			out.Segment1.ETHNICITY="HISPANIC";}
		else if (in.NewStatement.HISPANIC.#PCDATA=="No"){
			out.Segment1.ETHNICITY="NOT_HISPANIC";}
		else if (in.NewStatement.HISPANIC.#PCDATA=="Don't know"){
			out.Segment1.ETHNICITY="UNKNOWN";}}

}
