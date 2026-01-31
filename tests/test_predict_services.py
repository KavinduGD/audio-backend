import unittest
from unittest.mock import MagicMock, patch
from flask import Flask, request
from app.services.predict_services import PredictService


class TestPredictService(unittest.TestCase):

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
        cls.predict_service = PredictService(
            cls.mock_s3_client, cls.mock_sagemaker_client, cls.mock_dynamodb_client, cls.bucket_name, cls.role_arn)

    # 1. Test cases for `add_threshold`
    def test_add_threshold_missing_job_id(self):
        with self.app.test_request_context('/add-threshold', method='POST', json={'threshold': 0.5}):
            response, status_code = self.predict_service.add_threshold(request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # 2. Test cases for `remove_threshold`
    def test_remove_threshold_missing_job_id(self):
        with self.app.test_request_context('/remove-threshold', method='DELETE', json={}):
            response, status_code = self.predict_service.remove_threshold(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # 3. Test cases for `approve`
    def test_approve_missing_job_id(self):
        with self.app.test_request_context('/approve', method='POST', json={'approve_name': 'admin', 'approve_date': '2024-10-31'}):
            response, status_code = self.predict_service.approve(request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    def test_approve_endpoint_not_in_service(self):
        self.mock_dynamodb_client.get_item.return_value = {
            'Item': {'endpoint_name': {'S': 'endpoint-123'}}
        }
        self.mock_sagemaker_client.describe_endpoint.return_value = {
            'EndpointStatus': 'Creating'
        }
        with self.app.test_request_context('/approve', method='POST', json={
            'job_id': 'job-1234', 'approve_name': 'admin', 'approve_date': '2024-10-31',
            'display_names_for_training_classes': [{'class': 'class1', 'display_name': 'Class 1', 'icon': 'icon1', 'color': 'blue'}]
        }):
            response, status_code = self.predict_service.approve(request)
            self.assertEqual(status_code, 400)
            self.assertIn('Endpoint is not in service',
                          response.get_json()['message'])

    # 4. Test cases for `reject`
    def test_reject_missing_job_id(self):
        with self.app.test_request_context('/reject', method='POST', json={}):
            response, status_code = self.predict_service.reject(request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # 5. Test cases for `get_approved_jobs`
    def test_get_approved_jobs_success(self):
        self.mock_dynamodb_client.scan.return_value = {
            'Items': [{'job_id': {'S': 'job-1234'}, 'approve_name': {'S': 'admin'}}]
        }
        with self.app.test_request_context('/get_approved_jobs', method='GET'):
            with self.app.app_context():
                response, status_code = self.predict_service.get_approved_jobs(
                    request)
                self.assertEqual(status_code, 200)
                self.assertEqual(len(response.get_json()['jobs']), 1)

    # 6. Test cases for `predict_with_display_names`
    def test_predict_with_display_names_missing_job_id(self):
        with self.app.test_request_context('/predict-with-display-names', method='POST', data={'file': (None, 'test.wav')}):
            response, status_code = self.predict_service.predict_with_display_names(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    def test_predict_with_display_names_missing_file(self):
        with self.app.test_request_context('/predict-with-display-names', method='POST', data={'job_id': 'job-1234'}):
            response, status_code = self.predict_service.predict_with_display_names(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('File is required', response.get_json()['message'])

    def test_predict_with_display_names_job_not_found(self):
        self.mock_dynamodb_client.get_item.return_value = {}
        with self.app.test_request_context('/predict-with-display-names', method='POST', data={'job_id': 'job-1234', 'file': (None, 'test.wav')}):
            response, status_code = self.predict_service.predict_with_display_names(
                request)
            self.assertEqual(status_code, 404)
            self.assertIn('job_id does not exist',
                          response.get_json()['message'])


if __name__ == '__main__':
    unittest.main()
