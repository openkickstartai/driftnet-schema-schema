"""DriftNet tests — 9 cases covering extraction and drift comparison."""
import pytest
from driftnet import extract, compare

CODE = '''
import pandas as pd

df = pd.read_csv("users.csv")
names = df['user_id']
emails = df['email']
df.groupby('region')
df.sort_values(by=['created_at', 'score'])
df.merge(other, on='user_id')
result = df.astype({'score': float})

query = "SELECT user_id, email, name FROM users WHERE active = 1"

data = response.json()
config = data['settings']['theme']
'''


def test_extract_subscript():
    s = extract(CODE)
    assert "df" in s
    assert "user_id" in s["df"]["columns"]
    assert "email" in s["df"]["columns"]


def test_extract_groupby_sort():
    s = extract(CODE)
    cols = s["df"]["columns"]
    assert "region" in cols
    assert "created_at" in cols
    assert "score" in cols


def test_extract_merge_on():
    s = extract(CODE)
    assert "user_id" in s["df"]["columns"]
    refs = s["df"]["references"]["user_id"]
    assert len(refs) >= 2  # subscript + merge


def test_extract_sql():
    s = extract(CODE)
    assert "users" in s
    cols = s["users"]["columns"]
    assert "user_id" in cols
    assert "email" in cols
    assert "name" in cols


def test_extract_nested_dict():
    s = extract(CODE)
    assert "data" in s
    assert "settings" in s["data"]["columns"]


def test_extract_line_numbers():
    s = extract(CODE)
    refs = s["df"]["references"]["user_id"]
    assert all(isinstance(n, int) and n > 0 for n in refs)


def test_compare_no_drift():
    contract = {"t": {"columns": ["a", "b"], "references": {"a": [1], "b": [2]}}}
    actual = {"t": {"columns": ["a", "b", "c"]}}
    drifts = compare(contract, actual)
    assert not any(d["type"] == "missing" for d in drifts)


def test_compare_missing():
    contract = {"t": {"columns": ["a", "b"], "references": {"a": [1], "b": [2]}}}
    actual = {"t": {"columns": ["a"]}}
    drifts = compare(contract, actual)
    missing = [d for d in drifts if d["type"] == "missing"]
    assert len(missing) == 1
    assert missing[0]["column"] == "b"
    assert missing[0]["lines"] == [2]


def test_compare_added():
    contract = {"t": {"columns": ["a"], "references": {"a": [1]}}}
    actual = {"t": {"columns": ["a", "new_col"]}}
    drifts = compare(contract, actual)
    added = [d for d in drifts if d["type"] == "added"]
    assert len(added) == 1
    assert added[0]["column"] == "new_col"


def test_extract_empty():
    assert extract("x = 1\n") == {}
