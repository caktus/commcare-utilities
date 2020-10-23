from cc_utilities.common import make_commcare_export_sync_xl_wb


def test_make_commcare_export_xl_wb():
    "Show that `make_commcare_export_xl_wb` creates expected structure"
    mappings = {
        "contact": [
            ("first_name", "first_name"),
            ("last_name", "last_name"),
            ("source", "target"),
        ],
        "patient": [
            ("case_id", "case_id"),
            ("source", "target"),
        ],  # adding comment to avoid conflict between flake8 and black
    }

    wb = make_commcare_export_sync_xl_wb(mappings)
    assert set(wb.get_sheet_names()) == set(mappings.keys())
    for sheet in wb:
        assert sheet["A1"].value == "Data Source"
        assert sheet["A2"].value == "case"
        assert sheet["B1"].value == "Filter Name"
        assert sheet["B2"].value == "type"
        assert sheet["C1"].value == "Filter Value"
        assert sheet["C2"].value == sheet.title
        assert sheet["E1"].value == "Field"
        assert sheet["F1"].value == "Source Field"

        assert set(mappings[sheet.title]) == set(
            zip(
                map(lambda cell: cell.value, sheet["F"][1:]),
                map(lambda cell: cell.value, sheet["E"][1:]),
            )
        )
