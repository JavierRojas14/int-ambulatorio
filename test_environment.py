import sys
import unittest
import pandas as pd

from src.data.make_dataset import unir_filas_repetidas

REQUIRED_PYTHON = "python3"


class TestUnirFilasRepetidas(unittest.TestCase):

    def test_unir_filas_repetidas(self):
        # Test case 1: Simple example
        data = {
            'col1': [1, 1, 2, 2, 3],
            'col2': ['a', 'b', 'c', 'd', 'e'],
            'col3': ['x', 'y', 'z', 'w', 'v']
        }
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a, b', 'c, d', 'e'],
            'col3': ['x, y', 'z, w', 'v']
        })
        
        actual_result = unir_filas_repetidas(df, ['col1'], 'col2')
        pd.testing.assert_frame_equal(expected_result, actual_result)

        # Test case 2: More complex example
        data = {
            'col1': [1, 1, 1, 2, 2, 3],
            'col2': ['a', 'b', 'c', 'd', 'e', 'f'],
            'col3': ['x', 'y', 'z', 'w', 'v', 'u']
        }
        df = pd.DataFrame(data)
        expected_result = pd.DataFrame({
            'col1': [1, 2, 3],
            'col2': ['a, b, c', 'd, e', 'f'],
            'col3': ['x, y, z', 'w, v', 'u']
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
        raise ValueError("Unrecognized python interpreter: {}".format(
            REQUIRED_PYTHON))

    if system_major != required_major:
        raise TypeError(
            "This project requires Python {}. Found: Python {}".format(
                required_major, sys.version))
    else:
        print(">>> Development environment passes all tests!")


if __name__ == '__main__':
    unittest.main()
    main()
    
