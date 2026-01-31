import unittest
from unittest.mock import MagicMock, patch
from flask import Flask, request
from app.services.preprocess_data_services import PreprocessingService


class TestPreprocessingService(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.app = Flask(__name__)
        cls.app.config['TESTING'] = True
        cls.client = cls.app.test_client()
        cls.mock_s3_client = MagicMock()
        cls.mock_sagemaker_client = MagicMock()
        cls.mock_dynamodb_client = MagicMock()
        cls.bucket_name = 'test-bucket'
        cls.role_arn = 'arn:aws:iam::123456789012:role/SageMakerRole'
        cls.preprocess_image_uri = 'test-image-uri'
        cls.preprocess_service = PreprocessingService(
            cls.mock_s3_client, cls.mock_sagemaker_client, cls.mock_dynamodb_client, cls.bucket_name, cls.role_arn, cls.preprocess_image_uri)

    # def test_get_next_job_id_success(self):
    #     self.mock_dynamodb_client.query.return_value = {
    #         'Items': [{'job_id': {'S': 'job-1234'}}]
    #     }
    #     with self.app.app_context():
    #         response, status_code = self.preprocess_service.get_next_job_id()
    #         if isinstance(response, str):  # Adjust to parse as dict if response is string
    #             response = eval(response)
    #         self.assertEqual(status_code, 200)
    #         self.assertEqual(response['next_job_id'], 'job-1235')

    def test_get_single_jobs_data_missing_job_id(self):
        with self.app.test_request_context('/get-single-jobs-data', method='GET', query_string={}):
            response, status_code = self.preprocess_service.get_single_jobs_data(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    def test_preprocess_create_missing_params(self):
        with self.app.test_request_context('/preprocess-create', method='POST', json={}):
            response, status_code = self.preprocess_service.preprocess_create(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('Missing required fields', response.get_json()[
                          'message'])  # Updated to match actual message

    # def test_get_csv_file_success(self):
    #     self.mock_dynamodb_client.get_item.return_value = {
    #         'Item': {'job_name': {'S': 'test_job'}, 'sagemaker_preprocess_job_name': {'S': 'preprocess_job'}}
    #     }
    #     self.mock_s3_client.generate_presigned_url.return_value = 'http://example.com/presigned_url'

    #     with self.app.test_request_context('/get-csv-file', method='GET', query_string={'job_id': 'test_job_id'}):
    #         with self.app.app_context():
    #             response, status_code = self.preprocess_service.get_csv_file(
    #                 request)
    #             self.assertEqual(status_code, 200)
    #             self.assertIn('presigned_url', response.get_json())

    def test_delete_whole_job_missing_job_ids(self):
        with self.app.test_request_context('/delete-whole-job', method='DELETE', json={}):
            response, status_code = self.preprocess_service.delete_whole_job(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_ids is required',
                          response.get_json()['message'])

    # def test_preprocess_data_local_missing_job_id(self):
    #     with self.app.test_request_context('/preprocess-local', method='POST', json={}):
    #         response, status_code = self.preprocess_service.preprocess_data_local(
    #             request)
    #         self.assertEqual(status_code, 400)
    #         self.assertIn('job_id is required', response.get_json()['message'])

    # def test_preprocess_data_local_success(self):
    #     self.mock_dynamodb_client.get_item.return_value = {
    #         'Item': {
    #             'class_configs': {'L': [{'M': {'class_name': {'S': 'class1'}, 'class_count': {'N': '10'}, 'type': {'S': 'type1'}}}]},
    #             'job_name': {'S': 'test_job'}
    #         }
    #     }
    #     with patch('app.services.preprocess_data_services.docker.from_env') as mock_docker:
    #         mock_container = MagicMock()
    #         mock_container.logs.return_value = b"Container logs"
    #         mock_container.wait.return_value = {"StatusCode": 0}
    #         mock_docker.return_value.containers.run.return_value = mock_container

    #         with self.app.test_request_context('/preprocess-local', method='POST', json={'job_id': 'test_job_id'}):
    #             with self.app.app_context():
    #                 response, status_code = self.preprocess_service.preprocess_data_local(
    #                     request)
    #                 self.assertEqual(status_code, 201)
    #                 self.assertIn(
    #                     'Preprocessing job completed successfully.', response.get_json()['message'])


if __name__ == '__main__':
    unittest.main()
