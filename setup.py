from setuptools import setup

setup(
    name="commcare_utilities",
    version="1.0.0",
    description="Helpful utilities for working with CommCare",
    url="https://github.com/caktus/commcare-utilities",
    author="Benjamin White",
    author_email="pypy@benjamineugenewhite.com",
    license="MIT",
    packages=["cc_utilities", "cc_utilities.command_line"],
    install_requires=[
        "commcare-export",
        "retry",
        "dateparser",
        "openpyxl==2.5.12",  # commcare-export is pinned to this version
        "requests",
        "SQLAlchemy",
        "phonenumbers",
        "pandas",
        "numpy==1.19.3",  # windows env chokes on > than this version
        "xlrd",
    ],
    entry_points={
        "console_scripts": [
            "process-numbers-for-sms-capability=cc_utilities.command_line.process_numbers_for_sms_capability:main",
            "generate-case-export-query-file=cc_utilities.command_line.generate_case_export_query_file:main",
            "bulk-upload-legacy-contact-data=cc_utilities.command_line.bulk_upload_legacy_contact_data:main",
            "sync-commcare-app-to-db=cc_utilities.command_line.sync_commcare_app_to_db:main",
        ]
    },
    zip_safe=False,
)
