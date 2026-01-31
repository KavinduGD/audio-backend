import unittest
from unittest.mock import MagicMock
from flask import Flask, request
from app.services.zip_data_services import DataService
import io


class TestDataService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a Flask app and test client for testing Flask routes
        cls.app = Flask(__name__)
        cls.app.config['TESTING'] = True
        cls.client = cls.app.test_client()

        # Mock S3 client and bucket name
        cls.mock_s3_client = MagicMock()
        cls.bucket_name = 'test-bucket'

        # Initialize DataService with the mocked S3 client
        cls.data_service = DataService(cls.mock_s3_client, cls.bucket_name)

    # 1. Test cases for upload_zip_slow
    def test_upload_zip_slow_missing_file(self):
        with self.app.test_request_context('/upload-zip-slow', method='POST'):
            request.files = {}  # No file in the request
            response, status_code = self.data_service.upload_zip_slow(request)
            self.assertEqual(status_code, 400)
            self.assertIn('There are no file key in the request body',
                          response.get_json()['message'])

    def test_upload_zip_slow_invalid_class_name(self):
        with self.app.test_request_context('/upload-zip-slow', method='POST', data={'file': (io.BytesIO(b'some data'), 'sounds.zip')}):
            request.files = {'file': (io.BytesIO(b'some data'), 'sounds.zip')}
            response, status_code = self.data_service.upload_zip_slow(request)
            self.assertEqual(status_code, 400)
            self.assertIn('There are no class_name key in the request body',
                          response.get_json()['message'])

    # def test_upload_zip_slow_invalid_zip_structure(self):
    #     with self.app.test_request_context('/upload-zip-slow', method='POST', data={'class_name': 'test_class', 'file': (io.BytesIO(b'some data'), 'sounds.zip')}):
    #         request.files = {'file': (io.BytesIO(b'some data'), 'sounds.zip')}
    #         response, status_code = self.data_service.upload_zip_slow(request)
    #         self.assertEqual(status_code, 400)
    #         self.assertIn('Invalid file structure',
    #                       response.get_json()['message'])

    # 2. Test cases for upload_zip_fast
    def test_upload_zip_fast_missing_file(self):
        with self.app.test_request_context('/upload-zip-fast', method='POST'):
            request.files = {}  # No file in the request
            response, status_code = self.data_service.upload_zip_fast(request)
            self.assertEqual(status_code, 400)
            self.assertIn('There are no file key in the request body',
                          response.get_json()['message'])

    # def test_upload_zip_fast_invalid_zip_file(self):
    #     with self.app.test_request_context('/upload-zip-fast', method='POST', data={'class_name': 'test_class', 'file': (io.BytesIO(b'some data'), 'sounds.zip')}):
    #         request.files = {'file': (io.BytesIO(b'some data'), 'invalid.zip')}
    #         response, status_code = self.data_service.upload_zip_fast(request)
    #         self.assertEqual(status_code, 400)
    #         self.assertIn('The uploaded file name should be sounds.zip',
    #                       response.get_json()['message'])

    # 3. Test cases for get_class_count
    def test_get_class_count_missing_class_name(self):
        with self.app.test_request_context('/class-count', method='POST', json={}):
            response, status_code = self.data_service.get_class_count(request)
            self.assertEqual(status_code, 400)
            self.assertIn('There are no class_name key in the request body',
                          response.get_json()['message'])

    # def test_get_class_count_success(self):
    #     self.mock_s3_client.list_objects_v2.return_value = {'KeyCount': 5}
    #     with self.app.test_request_context('/class-count', method='POST', json={'class_name': 'test_class'}):
    #         with self.app.app_context():
    #             response, status_code = self.data_service.get_class_count(
    #                 request)
    #             self.assertEqual(status_code, 200)
    #             self.assertEqual(response.get_json()['count'], 5)

    # 4. Test cases for get_all_class_names_and_data_points
    def test_get_all_class_names_and_data_points(self):
        self.mock_s3_client.list_objects_v2.side_effect = [
            {'CommonPrefixes': [
                {'Prefix': 'zip_data/class1/'}, {'Prefix': 'zip_data/class2/'}]},
            {'KeyCount': 10},
            {'KeyCount': 20}
        ]
        with self.app.app_context():
            response, status_code = self.data_service.get_all_class_names_and_data_points()
            self.assertEqual(status_code, 200)
            self.assertEqual(response.get_json()['classes']['class1'], 10)
            self.assertEqual(response.get_json()['classes']['class2'], 20)

    # 5. Test cases for get_all_audios
    def test_get_all_audios_missing_class_name(self):
        with self.app.test_request_context('/get-all-audios', method='POST', json={}):
            response, status_code = self.data_service.get_all_audios(request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name is required',
                          response.get_json()['message'])

    # def test_get_all_audios_success(self):
    #     self.mock_s3_client.list_objects_v2.return_value = {'Contents': [
    #         {'Key': 'zip_data/class1/file.wav', 'LastModified': '2024-01-01T12:00:00Z', 'Size': 12345}]}
    #     with self.app.test_request_context('/get-all-audios', method='POST', json={'class_name': 'class1'}):
    #         with self.app.app_context():
    #             response, status_code = self.data_service.get_all_audios(
    #                 request)
    #             self.assertEqual(status_code, 200)
    #             self.assertIn('audio_files', response.get_json())

    # 6. Test cases for get_random_audios
    def test_get_random_audios_missing_class_name_or_count(self):
        with self.app.test_request_context('/get-random-audios', method='POST', json={'count': 3}):
            response, status_code = self.data_service.get_random_audios(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name and count is required',
                          response.get_json()['message'])

    # 7. Test cases for delete_all_audios_from_set_of_classes
    def test_delete_all_audios_from_set_of_classes_empty_list(self):
        with self.app.test_request_context('/delete-all-audios-from-set-of-classes', method='DELETE', json={'class_names': []}):
            response, status_code = self.data_service.delete_all_audios_from_set_of_classes(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('A list of class names is required',
                          response.get_json()['message'])

    # 8. Test cases for delete_all_class_audios
    def test_delete_all_class_audios_missing_class_name(self):
        with self.app.test_request_context('/delete-all-class-audios', method='DELETE', json={}):
            response, status_code = self.data_service.delete_all_class_audios(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name is required',
                          response.get_json()['message'])

    # 9. Test cases for delete_percentage_audios
    def test_delete_percentage_audios_missing_class_name_or_percentage(self):
        with self.app.test_request_context('/delete-percentage-audios', method='DELETE', json={'percentage': 50}):
            response, status_code = self.data_service.delete_percentage_audios(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name and valid percentage are required',
                          response.get_json()['message'])

    def test_delete_percentage_audios_invalid_percentage(self):
        with self.app.test_request_context('/delete-percentage-audios', method='DELETE', json={'class_name': 'class1', 'percentage': 'invalid'}):
            response, status_code = self.data_service.delete_percentage_audios(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name and valid percentage are required',
                          response.get_json()['message'])


if __name__ == '__main__':
    unittest.main()
