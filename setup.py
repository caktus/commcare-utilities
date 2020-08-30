from setuptools import setup

setup(
    name="commcare_utilities",
    version="0.1",
    description="Helpful utilities for working with CommCare",
    url="https://github.com/caktus/commcare-utilities",
    author="Benjamin White",
    author_email="ben@benjamineugenewhite.com",
    # license="MIT",
    packages=["common"],
    install_requires=[
        "openpyxl<=2.5.12",
        "requests",
        "commcare-export",
        "pre-commit",
        "SQLAlchemy",
        "phonenumbers",
        "pandas",
        "psycopg2"
    ],
    scripts=["scripts/lookup_contact_phone_numbers", "scripts/sync_commcare_to_db"],
    zip_safe=False,
)