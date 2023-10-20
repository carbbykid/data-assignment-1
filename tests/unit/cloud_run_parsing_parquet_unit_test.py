import pytest
from batch_job.cloud_run_batch_job.main import _transform_event_attribute

test_data = [
    (
        "test_purchases_event",
        {"revenue": 123.0, "transaction_id": "3124wfdb6332asdc1332"},
        [
            {
                "key": "revenue",
                "int_value": None,
                "float_value": 123.0,
                "string_value": None,
                "bool_value": None,
            },
            {
                "key": "transaction_id",
                "int_value": None,
                "float_value": None,
                "string_value": "3124wfdb6332asdc1332",
                "bool_value": None,
            },
        ],
    ),
    (
        "test_view_event",
        {"creative_id": 1, "view_time": 13, "is_click": False},
        [
            {
                "key": "creative_id",
                "int_value": 1,
                "float_value": None,
                "string_value": None,
                "bool_value": None,
            },
            {
                "key": "view_time",
                "int_value": 13,
                "float_value": None,
                "string_value": None,
                "bool_value": None,
            },
            {
                "key": "is_click",
                "int_value": None,
                "float_value": None,
                "string_value": None,
                "bool_value": False,
            },
        ],
    ),
    (
        "test_log_in_event",
        [],
        [],
    ),
    (
        "test_play_event",
        {"play_time": 239},
        [
            {
                "key": "play_time",
                "int_value": 239,
                "float_value": None,
                "string_value": None,
                "bool_value": None,
            },
        ],
    ),
]


@pytest.mark.parametrize(
    "test_name,event_attribute,expected", 
    test_data, 
    ids=[test[0] for test in test_data]
)
def test_transform_event_attribute(test_name, event_attribute, expected):
    print(f"==>> event_attribute: {event_attribute}")
    result = _transform_event_attribute(event_attribute)
    len(result) == len(expected)
    for item in expected:
        assert item in result
