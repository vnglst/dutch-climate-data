from unittest.mock import patch, Mock
from datetime import datetime
import os
import unittest
import json
from unittest.mock import patch, Mock
import update_temperatures as ut


class MockDateTime:
    @classmethod
    def now(cls):
        return datetime(2023, 1, 1)


class TestUpdateTemperatures(unittest.TestCase):
    @patch('requests.get')
    @patch('datetime.datetime', new=MockDateTime)
    def test_main(self, mock_get):
        with open('mocks/mndgeg_260_tg.txt', 'r') as file:
            mocked_data = file.read()

        mock_response = Mock()
        mock_response.text = mocked_data
        mock_get.return_value = mock_response

        # Run the main function
        ut.main()

        # Assert that the files were created
        self.assertTrue(os.path.exists('data/temperature-heatmap.json'))
        self.assertTrue(os.path.exists('data/temperature-anomalies.json'))

        # Read and parse the data from the files
        with open('data/temperature-heatmap.json', 'r') as file:
            heatmap_data = json.load(file)
        with open('data/temperature-anomalies.json', 'r') as file:
            anomalies_data = json.load(file)

        # Assert that the files were created
        self.assertTrue(os.path.exists('data/temperature-heatmap.json'))
        self.assertTrue(os.path.exists('data/temperature-anomalies.json'))

        # Assert that the timestamps match
        self.assertEqual(heatmap_data['timestamp'],
                         '2023-01-01T00:00:00')
        self.assertEqual(anomalies_data['timestamp'],
                         '2023-01-01T00:00:00')

        # Assert that first data line matches
        self.assertEqual(heatmap_data['data'][0], [11, '2023', None])
        self.assertEqual(anomalies_data['data'][0], -0.5649999999999995)

        # Assert that last data line matches
        self.assertEqual(heatmap_data['data'][-1],
                         [0, "1901", -2.4809999999999985])
        self.assertEqual(anomalies_data['data'][-1], None)
