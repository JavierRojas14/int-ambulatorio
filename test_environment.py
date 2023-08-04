import sys
import unittest
import pandas as pd

from src.data.make_dataset import unir_filas_repetidas

REQUIRED_PYTHON = "python3"


class TestUnirFilasRepetidas(unittest.TestCase):
    def test_unir_filas_repetidas_empty_dataframe(self):
        # Test case: Empty DataFrame
        df = pd.DataFrame()
        expected_result = pd.DataFrame()
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_unir_filas_repetidas_no_duplicates(self):
        # Test case: No duplicate values in columnas_repetidas
        data = {"col1": [1, 2, 3], "col2": ["a", "b", "c"], "col3": ["x", "y", "z"]}
        df = pd.DataFrame(data)
        expected_result = df[["col1", "col2"]]
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_unir_filas_repetidas_single_distinct_value(self):
        # Test case: Single distinct value in columna_distinta
        data = {"col1": [1, 1, 2, 2], "col2": ["a", "b", "c", "d"], "col3": ["x", "x", "y", "y"]}
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({"col1": [1, 2], "col2": ["a, b", "c, d"]})
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_unir_filas_repetidas_multiple_distinct_values(self):
        # Test case: Multiple distinct values in columna_distinta
        data = {
            "col1": [1, 1, 2, 2, 3, 3],
            "col2": ["a", "b", "c", "d", "e", "f"],
            "col3": ["x", "y", "z", "w", "x", "y"],
        }
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a, b", "c, d", "e, f"]})
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_unir_filas_repetidas_non_string_values(self):
        # Test case: Non-string values in columna_distinta
        data = {"col1": [1, 1, 2, 2], "col2": ["a", "b", "c", "d"], "col3": [10, 10, 20, 20]}
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({"col1": [1, 2], "col2": ["a, b", "c, d"]})
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)

    def test_unir_filas_repetidas_missing_values(self):
        # Test case: Handling missing values (NaN)
        data = {
            "col1": [1, 1, 2, 2, None, None],
            "col2": ["a", "b", "c", "d", "e", "f"],
            "col3": ["x", "x", "y", "y", "z", "z"],
        }
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({"col1": [1, 2, None], "col2": ["a, b", "c, d", "e, f"]})
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)
    
    def test_unir_filas_repetidas_multiple_rows(self):
        # Test case: Multiple duplicate values in columna_distinta, joining more than 2 rows
        data = {
            'col1': [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4],
            'col2': ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l'],
            'col3': ['x', 'y', 'z', 'w', 'v', 'u', 'x', 'y', 'z', 'w', 'v', 'u']
        }
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({
            'col1': [1, 2, 3, 4],
            'col2': ['a, b, c', 'd, e, f', 'g, h, i', 'j, k, l']
        })
        actual_result = unir_filas_repetidas(df, ['col1'], 'col2')
        pd.testing.assert_frame_equal(expected_result, actual_result)


def main():
    system_major = sys.version_info.major
    if REQUIRED_PYTHON == "python":
        required_major = 2
    elif REQUIRED_PYTHON == "python3":
        required_major = 3
    else:
        raise ValueError("Unrecognized python interpreter: {}".format(REQUIRED_PYTHON))

    if system_major != required_major:
        raise TypeError(
            "This project requires Python {}. Found: Python {}".format(required_major, sys.version)
        )
    else:
        print(">>> Development environment passes all tests!")


if __name__ == "__main__":
    unittest.main()
    main()
