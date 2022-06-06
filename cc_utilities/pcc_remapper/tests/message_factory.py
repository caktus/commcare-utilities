import random
from pathlib import Path

from bs4 import BeautifulSoup
from faker import Faker

from cc_utilities import constants

fake = Faker()
Faker.seed(random.choice([18274987, 2878, 897238]))

FAKED_MESSAGES_FILE = Path("faked_messages.xml")

TEST_CHOICES = (
    constants.TEST_RESULTS_CONFIRMED
    + constants.TEST_RESULTS_PROBABLE
    + constants.TEST_RESULTS_NO_SEND
)
RESULT_CHOICES = constants.RESULT_VALUES_SEND + ["Negative"]


def _generate_message():
    message = f"""
    <message>
    <NewStatement>
        <CASE_ID>{fake.unique.random_int()}</CASE_ID>
        <BIRTH_DATE>{fake.date()}</BIRTH_DATE>
        <FIRST_NAME>{fake.first_name()}</FIRST_NAME>
        <LAST_NAME>{fake.last_name()}</LAST_NAME>
        <STREET1>{fake.street_address()}</STREET1>
        <STREET2>{str("Appt 3") if fake.pybool() else ""}</STREET2>
        <POSTAL_CODE>{fake.postcode()}</POSTAL_CODE>
        <CITY>{fake.city()}</CITY>
        <STATE>PA</STATE>
        <HOME_PHONE>{fake.phone_number()}</HOME_PHONE>
        <MOBILE_PHONE>{fake.phone_number() if fake.pybool() else ""}</MOBILE_PHONE>
        <EMAIL>{fake.email() if fake.pybool() else ""}</EMAIL>
        <GENDER>{fake.random_element(constants.GENDER_MAP.keys())}</GENDER>
        <RACE>{fake.random_element(constants.RACE_MAP.keys())}</RACE>
        <RESULT>{fake.random_element(RESULT_CHOICES)}</RESULT>
        <TEST>{fake.random_element(TEST_CHOICES)}</TEST>
        <RESULT_VALUE><RESULT_VALUE>
        <SPECIMEN_DATE>{fake.date()}</SPECIMEN_DATE>
        <FACILITY__OTHER>HUP Hospital of the</FACILITY__OTHER>
        <UNID>{fake.unique.random_int()}</UNID>
        <AGE_YEARS>{fake.pydecimal(positive=True, left_digits=3, right_digits=5, max_value=75.00)}</AGE_YEARS>
        <HISPANIC>{'Yes' if fake.pybool() else 'No'}</HISPANIC>
        <RESULT_DATE>{fake.date()}</RESULT_DATE>
        <FACILITY>HOSP UNIV OF PENN LAB,PENN-HUP-LAB,3400 SPRUCE ST,PHILADELPHIA,PA,19104,215-662-3406,LB6005</FACILITY>
        <ORDER_FACILITY>HOSP UNIV OF PENN LAB,PENN-HUP-LAB,3400 SPRUCE ST,PHILADELPHIA,PA,19104,215-662-3406,LB6005</ORDER_FACILITY>
        <ORDER_PROVIDER>{fake.last_name()}, {fake.first_name()}</ORDER_PROVIDER>
        <CREATE_DATE>{fake.date()}</CREATE_DATE>
        <COMMCARE_ID>{fake.unique.random_int() if fake.pybool() else ""}</COMMCARE_ID>
        <identifier>{fake.unique.random_int()}</identifier>
    </NewStatement>
    </message>
    """
    return message


def generate_message_file(num_messages=20):
    soup = BeautifulSoup(features="xml")
    for x in range(num_messages):
        soup.append(_generate_message())
    with FAKED_MESSAGES_FILE.open("w", encoding="utf-8") as fh:
        fh.write(str(soup.prettify(formatter=None)))


if __name__ == "__main__":
    generate_message_file()
