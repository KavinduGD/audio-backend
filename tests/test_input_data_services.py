import unittest
from unittest.mock import MagicMock, patch
from flask import Flask, request
from app.services.input_data_services import DataService


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

    # Test cases for `compare`
    def test_compare_missing_class_names(self):
        with self.app.test_request_context('/compare', method='POST', json={}):
            response, status_code = self.data_service.compare(request)
            self.assertEqual(status_code, 400)
            self.assertIn("zip_class_name and input_class_name are required",
                          response.get_json()['message'])

    def test_compare_success(self):
        self.mock_s3_client.list_objects_v2.side_effect = [
            {'Contents': [{'Key': 'zip_data/class1/file1.wav'},
                          {'Key': 'zip_data/class1/file2.wav'}]},
            {'Contents': [{'Key': 'input_data/class2/file2.wav'},
                          {'Key': 'input_data/class2/file3.wav'}]}
        ]
        with self.app.test_request_context('/compare', method='POST', json={'zip_class_name': 'class1', 'input_class_name': 'class2'}):
            with self.app.app_context():
                response, status_code = self.data_service.compare(request)
                self.assertEqual(status_code, 200)
                self.assertEqual(response.get_json()['status'], 'success')
                self.assertIn('file2.wav', response.get_json()
                              ['common_audio_files'])

    # Test cases for `rename`
    def test_rename_missing_class_names(self):
        with self.app.test_request_context('/rename', method='POST', json={}):
            response, status_code = self.data_service.rename(request)
            self.assertEqual(status_code, 400)
            self.assertIn("zip_class_name and input_class_name are required",
                          response.get_json()['message'])

    def test_rename_success(self):
        self.mock_s3_client.list_objects_v2.side_effect = [
            {'Contents': [{'Key': 'zip_data/class1/file1.wav'},
                          {'Key': 'zip_data/class1/file2.wav'}]},
            {'Contents': [{'Key': 'input_data/class2/file2.wav'},
                          {'Key': 'input_data/class2/file3.wav'}]}
        ]
        self.mock_s3_client.copy_object.return_value = {}
        self.mock_s3_client.delete_object.return_value = {}

        with self.app.test_request_context('/rename', method='POST', json={'zip_class_name': 'class1', 'input_class_name': 'class2'}):
            with self.app.app_context():
                response, status_code = self.data_service.rename(request)
                self.assertEqual(status_code, 200)
                self.assertEqual(response.get_json()['status'], 'success')
                self.assertIn('renamed_files', response.get_json())

    # Test cases for `get_class_count`
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

    # Test cases for `get_all_class_names_and_data_points`
    def test_get_all_class_names_and_data_points(self):
        self.mock_s3_client.list_objects_v2.side_effect = [
            {'CommonPrefixes': [
                {'Prefix': 'input_data/class1/'}, {'Prefix': 'input_data/class2/'}]},
            {'KeyCount': 10},
            {'KeyCount': 20}
        ]
        with self.app.app_context():
            response, status_code = self.data_service.get_all_class_names_and_data_points()
            self.assertEqual(status_code, 200)
            self.assertEqual(response.get_json()['classes']['class1'], 10)
            self.assertEqual(response.get_json()['classes']['class2'], 20)

    # Test cases for `get_all_audios`
    def test_get_all_audios_missing_class_name(self):
        with self.app.test_request_context('/get-all-audios', method='POST', json={}):
            response, status_code = self.data_service.get_all_audios(request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name is required',
                          response.get_json()['message'])

    # def test_get_all_audios_success(self):
    #     self.mock_s3_client.list_objects_v2.return_value = {'Contents': [
    #         {'Key': 'input_data/class1/file1.wav', 'LastModified': '2024-01-01T12:00:00Z', 'Size': 12345}]}
    #     with self.app.test_request_context('/get-all-audios', method='POST', json={'class_name': 'class1'}):
    #         with self.app.app_context():
    #             response, status_code = self.data_service.get_all_audios(
    #                 request)
    #             self.assertEqual(status_code, 200)
    #             self.assertIn('audio_files', response.get_json())

    # Test cases for `get_random_audios`
    def test_get_random_audios_missing_class_name_or_count(self):
        with self.app.test_request_context('/get-random-audios', method='POST', json={'count': 3}):
            response, status_code = self.data_service.get_random_audios(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('class name and count is required',
                          response.get_json()['message'])

    # Test cases for `copy_and_keep_both_with_percentage`
    def test_copy_and_keep_both_with_percentage_missing_params(self):
        with self.app.test_request_context('/copy-and-keep-both-with-percentage', method='POST', json={'zip_class_name': 'class1', 'percentage': 50}):
            response, status_code = self.data_service.copy_and_keep_both_with_percentage(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('zip_class_name, input_class_name and valid percentage are required',
                          response.get_json()['message'])

    def test_copy_and_keep_both_with_percentage_success(self):
        self.mock_s3_client.list_objects_v2.side_effect = [
            {'Contents': [{'Key': 'zip_data/class1/file1.wav'},
                          {'Key': 'zip_data/class1/file2.wav'}]},
            {'KeyCount': 3}
        ]
        self.mock_s3_client.copy_object.return_value = {}
        with self.app.test_request_context('/copy-and-keep-both-with-percentage', method='POST', json={'zip_class_name': 'class1', 'input_class_name': 'class2', 'percentage': 50}):
            with self.app.app_context():
                response, status_code = self.data_service.copy_and_keep_both_with_percentage(
                    request)
                self.assertEqual(status_code, 200)
                self.assertEqual(response.get_json()['status'], 'success')

    # Test cases for `delete_all_audios_from_set_of_classes`
    def test_delete_all_audios_from_set_of_classes_empty_list(self):
        with self.app.test_request_context('/delete-all-audios-from-set-of-classes', method='DELETE', json={'class_names': []}):
            response, status_code = self.data_service.delete_all_audios_from_set_of_classes(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('A list of class names is required',
                          response.get_json()['message'])


if __name__ == '__main__':
    unittest.main()
