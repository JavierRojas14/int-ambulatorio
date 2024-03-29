import sys
import unittest
import pandas as pd
import numpy as np

from src.data.make_dataset import unir_filas_repetidas
from src.features.build_features import conteo_agrupado_de_variable

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
            "col1": [1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4],
            "col2": ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l"],
            "col3": ["x", "y", "z", "w", "v", "u", "x", "y", "z", "w", "v", "u"],
        }
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame(
            {"col1": [1, 2, 3, 4], "col2": ["a, b, c", "d, e, f", "g, h, i", "j, k, l"]}
        )
        actual_result = unir_filas_repetidas(df, ["col1"], "col2")
        pd.testing.assert_frame_equal(expected_result, actual_result)


class TestConteoAgrupadoDeVariable(unittest.TestCase):
    def setUp(self):
        self.sample_data = {
            "Category": ["A", "B", "A", "C", "B", "B"],
            "ICD-10": ["A00", "B01", "C02", "D03", "A00", "A00"],
            "Gender": ["M", "F", "M", "F", "F", "F"],
            "ID_1": [1001, 1002, 1001, 1003, 1002, 1001],
            "ID_2": [2001, 2001, 2002, 2003, 2002, 2001],
        }

    def test_basic_scenario(self):
        df = pd.DataFrame(self.sample_data)
        result = conteo_agrupado_de_variable(
            df, ["Category"], "ICD-10", ["Category", "ICD-10"], "Counts"
        )

        expected_data = {
            "Category": ["A", "A", "B", "B", "C"],
            "ICD-10": ["A00", "C02", "A00", "B01", "D03"],
            "conteo_Counts": [1, 1, 2, 1, 1],
            "llave_id": ["A-A00", "A-C02", "B-A00", "B-B01", "C-D03"],
        }
        expected_result = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result, expected_result)

    def test_empty_dataframe(self):
        empty_df = pd.DataFrame(columns=["Category", "ICD-10", "Gender", "ID_1", "ID_2"])
        empty_result = conteo_agrupado_de_variable(
            empty_df, ["Category"], "ICD-10", ["Category", "ICD-10"], "Counts"
        )

        self.assertEqual(empty_result, None)

    def test_missing_values(self):
        missing_data = {
            "Category": ["A", "A", "B", "B", np.nan],
            "ICD-10": ["A00", "B01", "C02", "D03", "E04"],
            "Gender": ["M", "F", "M", "F", "F"],
            "ID_1": [1001, 1002, 1001, 1002, 1001],
            "ID_2": [2001, 2001, 2002, 2002, 2001],
        }
        missing_df = pd.DataFrame(missing_data)
        missing_result = conteo_agrupado_de_variable(
            missing_df, ["Category"], "ICD-10", ["Category", "ICD-10"], "Counts"
        )
        expected_missing_data = {
            "Category": ["A", "A", "B", "B", np.nan],
            "ICD-10": ["A00", "B01", "C02", "D03", "E04"],
            "conteo_Counts": [1, 1, 1, 1, 1],
            "llave_id": ["A-A00", "A-B01", "B-C02", "B-D03", "nan-E04"],
        }
        expected_missing_result = pd.DataFrame(expected_missing_data)
        pd.testing.assert_frame_equal(missing_result, expected_missing_result)

    def test_duplicates_in_grouping(self):
        duplicate_data = {
            "Category": ["A", "A", "B", "B", "A"],
            "ICD-10": ["A00", "B01", "C02", "D03", "A00"],
            "Gender": ["M", "F", "M", "F", "M"],
            "ID_1": [1001, 1002, 1001, 1002, 1001],
            "ID_2": [2001, 2001, 2002, 2002, 2001],
        }
        duplicate_df = pd.DataFrame(duplicate_data)
        result = conteo_agrupado_de_variable(
            duplicate_df,
            ["Category", "Gender"],
            "ICD-10",
            ["Category", "Gender", "ICD-10"],
            "Counts",
        )

        expected_data = {
            "Category": ["A", "A", "B", "B"],
            "Gender": ["F", "M", "F", "M"],
            "ICD-10": ["B01", "A00", "D03", "C02"],
            "conteo_Counts": [1, 2, 1, 1],
            "llave_id": ["A-F-B01", "A-M-A00", "B-F-D03", "B-M-C02"],
        }
        expected_result = pd.DataFrame(expected_data)
        pd.testing.assert_frame_equal(result, expected_result)


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
