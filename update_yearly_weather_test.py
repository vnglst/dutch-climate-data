import unittest
from unittest.mock import patch, Mock
import io
import zipfile
import os
import update_yearly_weather
import json

OUTPUT_FILE = 'tmp/weather.json'


class TestUpdateRainfall(unittest.TestCase):
    def setUp(self):
        # make sure the output directory exists
        os.makedirs('tmp', exist_ok=True)
        with open('mocks/etmgeg_260.zip', 'rb') as f:
            self.mock_zip_content = f.read()

    @patch('requests.get')
    def test_main(self, mock_get):
        mock_response = Mock()
        mock_response.content = self.mock_zip_content
        mock_get.return_value = mock_response

        update_yearly_weather.main('localhost:3000/mock', OUTPUT_FILE)

        self.assertTrue(os.path.exists(OUTPUT_FILE))

        with open(OUTPUT_FILE, 'r') as file:
            data = json.load(file)

        # 1906
        self.assertEqual(data['temperature_anomalies']
                         [0], -0.18190688713699643)
        # 2022
        self.assertEqual(data['temperature_anomalies'][-1], 2.4676821539588936)
        self.assertEqual(data['mean_temperature'], 9.323276750150695)

        self.assertEqual(data['years'][0], '1906')
        self.assertEqual(data['years'][-1], '2023')


if __name__ == '__main__':
    unittest.main()
