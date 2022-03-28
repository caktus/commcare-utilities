import re
from dataclasses import dataclass
from enum import Enum
from typing import Any


class FacilityTypes(Enum):
    LAB = "lab"
    ORDERING = "ordering"


@dataclass
class PccRecord:
    external_id: int
    first_name: str
    last_name: str
    street1: str
    city: str
    county: str
    postal_code: Any
    mobile_phone: Any
    home_phone: str
    age_years: Any
    test_type: Any
    create_date: Any
    state: str
    birth_date: Any
    test: Any
    race: str
    result: Any
    gender: str
    result_date: Any
    facility: Any
    order_facility: Any
    specimen_date: Any
    order_provider: Any
    case_id: int
    hispanic: Any

    def get_map(self):
        return {
            "FULL_NAME": f"{self.first_name} {self.last_name}",
            "NAME": f"{self.first_name} {self.last_name}",
            "ADDRESS": self.street1,  # We might want to do some normalization here
            "ADDRESS_CITY": self.city,
            "ADDRESS_COUNTY": self.county if self.county else self.city,
            "AGE": self.get_age(),
            "TEST_TYPE": self.test,
            "CDMS_CREATE_DATE": self.create_date,
            "CASE_IMPORT_DATE": self.create_date,
            "ADDRESS_STATE": self.state,
            "ADDRESS_ZIP": self.postal_code,
            "FIRST_NAME": self.first_name,
            "LAST_NAME": self.last_name,
            "DOB": self.birth_date,
            "RACE": self.race,
            "LAB_RESULT": self.result,
            "GENDER": self.gender,
            "ANALYSIS_DATE": self.result_date,
            "LAB_FACILITY": self.get_facilities(FacilityTypes.LAB),
            "ORDERING_FACILITY": self.get_facilities(FacilityTypes.ORDERING),
            "SPECIMEN_COLLECTION_DATE": self.specimen_date,
            "CDMS_ID": self.case_id,
            "PHONE_HOME": self.get_phone(),
            "ETHNICITY": self.get_ethnicity()
        }

    def get_age(self):
        return self.age_years.split(".")[1]

    def get_facilities(self, facility_type):
        facility = None
        if facility_type == FacilityTypes.LAB:
            facility = self.facility.split(',')[1]
        if facility_type == FacilityTypes.ORDERING:
            facility = self.order_facility.split(',')[1]
        return facility

    def get_phone(self):
        phone = None
        clean = re.compile(r"[(,),\s,-]")
        if self.mobile_phone:
            phone = re.sub(clean, "", self.mobile_phone)
        if self.home_phone:
            phone = re.sub(clean, "", self.home_phone)
        return phone
