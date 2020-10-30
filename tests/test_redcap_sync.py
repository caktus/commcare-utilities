import pandas as pd

from cc_utilities.redcap_sync import collapse_checkbox_columns


def test_collapse_checkbox_columns():
    df = pd.DataFrame(
        {
            "box1___yellow": [1, None, 1],
            "box1___green": [None, 1, 1],
            "box1___blue": [None, None, None],
            "box1___other": [1, None, None],
            "box1__other": ["test", "", ""],
        }
    )
    expected_df = pd.DataFrame(
        {
            "box1__other": ["test", "", ""],
            "box1": ["yellow other", "green", "yellow green"],
        }
    )
    new_df = collapse_checkbox_columns(df)
    pd.testing.assert_frame_equal(expected_df, new_df)
