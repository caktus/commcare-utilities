from setuptools import setup

setup(
    name="commcare_utilities",
    version="0.1",
    description="Helpful utilities for working with CommCare",
    url="https://github.com/caktus/commcare-utilities",
    author="Benjamin White",
    author_email="ben@benjamineugenewhite.com",
    # license="MIT",   if/when we open source this, need to set right license type
    packages=["cc_utilities"],
    install_requires=[
        # commcare-export is a dependency of this package and it's pinned at 2.5.12
        # so we do this to avoid version conflict.
        "openpyxl<=2.5.12",
        "requests",
        "commcare-export",
        "pre-commit",
        "SQLAlchemy",
        "phonenumbers",
        "pandas",
    ],
    entry_points={
        "console_scripts": [
            "process-numbers-for-sms-capability=cc_utilities.command_line.process_numbers_for_sms_capability:main",
            "sync-commcare-to-db=cc_utilities.command_line.sync_commcare_to_db:main",
        ]
    },
    zip_safe=False,
)
