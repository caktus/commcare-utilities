import logging

logger = logging.getLogger(__name__)


def collapse_checkbox_columns(df):
    """
    Converts REDCap-style checkbox columns ("box1___yellow", "box1___green") to CommCare-style
    checkbox files ("box1": "yellow green"). See test for details.
    """
    df = df.copy()
    for redcap_col in [col for col in df.columns if "___" in col]:
        cc_property, cc_value = redcap_col.split("___", 1)
        if cc_property not in df.columns:
            logger.info(f"Adding column {cc_property} to df")
            df[cc_property] = ""
        # Covert NaN/0/1 in REDCap to space-separated string values for CommCare
        df[cc_property] = df[cc_property].combine(
            df[redcap_col].fillna(0),
            lambda existing_cc_value, redcap_is_set: " ".join(
                filter(bool, [existing_cc_value, cc_value if redcap_is_set else ""])
            ),
        )
        logger.info(f"Dropping column {redcap_col} from df")
        df.drop(columns=redcap_col, inplace=True)
    return df


def clean_redcap_data(df):
    """
    Cleans a dataframe from REDCap to be suitable for import to CommCare.
    """
    df = collapse_checkbox_columns(df)
    return df
