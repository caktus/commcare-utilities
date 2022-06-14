from setuptools import setup

setup(
    name="commcare_utilities",
    version="1.0.0",
    description="Helpful utilities for working with CommCare",
    url="https://github.com/caktus/commcare-utilities",
    author="Caktus Consulting Group, LLC",
    author_email="team@caktusgroup.com",
    license="MIT",
    packages=[
        "cc_utilities",
        "cc_utilities.command_line",
        "cc_utilities.pcc_remapper",
        "cc_utilities.pcc_remapper.data_objects",
        "cc_utilities.pcc_remapper.database",
        "cc_utilities.pcc_remapper.factories",
        "cc_utilities.pcc_remapper.support_scripts",
    ],
    install_requires=[
        "commcare-export~=1.8.1",
        "retry",
        "dateparser",
        "openpyxl",  # commcare-export is pinned to this version
        "requests",
        "SQLAlchemy",
        "phonenumbers",
        "pandas",
        "pyodbc",
        "pyyaml",
        "pycap",  # REDCap API
        "numpy==1.19.3",  # windows env chokes on > than this version
        "xlrd",
        "peewee==3.14.10",
        "faker",
        "beautifulsoup4",
        "requests",
    ],
    entry_points={
        "console_scripts": [
            "process-numbers-for-sms-capability=cc_utilities.command_line.process_numbers_for_sms_capability:main",
            "generate-case-export-query-file=cc_utilities.command_line.generate_case_export_query_file:main",
            "bulk-upload-legacy-contact-data=cc_utilities.command_line.bulk_upload_legacy_contact_data:main",
            "sync-commcare-app-to-db=cc_utilities.command_line.sync_commcare_app_to_db:main",
            "easy-bulk-upload-contacts=cc_utilities.command_line.user_friendly_bulk_contact_upload:main",
            "sync-redcap-to-commcare=cc_utilities.command_line.sync_redcap_to_commcare:main",
            "xml_receiver=cc_utilities.pcc_remapper.support_scripts.xml_receiver:main",
        ]
    },
    zip_safe=False,
)
