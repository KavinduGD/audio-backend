import unittest
from unittest.mock import MagicMock, patch
from flask import Flask, request
from app.services.deploy_model_services import DeployingService


class TestDeployingService(unittest.TestCase):

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
        cls.deploy_service = DeployingService(
            cls.mock_s3_client, cls.mock_sagemaker_client, cls.mock_dynamodb_client, cls.bucket_name, cls.role_arn)

    # 1. Test cases for `add_deployment_details`
    def test_add_deployment_details_missing_job_id(self):
        with self.app.test_request_context('/add_deployment_details', method='POST', json={
            'deploy_instance_type': 'ml.m5.large', 'deploy_instance_count': 1, 'deploy_date': '2024-10-31'
        }):
            response, status_code = self.deploy_service.add_deployment_details(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    def test_add_deployment_details_invalid_instance_count(self):
        with self.app.test_request_context('/add_deployment_details', method='POST', json={
            'job_id': 'job-1234', 'deploy_instance_type': 'ml.m5.large', 'deploy_instance_count': 10, 'deploy_date': '2024-10-31'
        }):
            response, status_code = self.deploy_service.add_deployment_details(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('Instance count must be between 1 and 5',
                          response.get_json()['message'])

    # 2. Test cases for `delete_deployment_details`
    def test_delete_deployment_details_missing_job_id(self):
        with self.app.test_request_context('/delete_deployment_details', method='DELETE', query_string={}):
            response, status_code = self.deploy_service.delete_deployment_details(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # 3. Test cases for `deploy_model`
    def test_deploy_model_missing_job_id(self):
        with self.app.test_request_context('/deploy_model', method='POST', json={}):
            response, status_code = self.deploy_service.deploy_model(request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    def test_deploy_model_training_in_progress(self):
        self.mock_dynamodb_client.get_item.return_value = {
            'Item': {'sagemaker_train_job_name': {'S': 'train-job'}}
        }
        self.mock_sagemaker_client.describe_training_job.return_value = {
            'TrainingJobStatus': 'InProgress'
        }
        with self.app.test_request_context('/deploy_model', method='POST', json={'job_id': 'job-1234'}):
            response, status_code = self.deploy_service.deploy_model(request)
            self.assertEqual(status_code, 400)
            self.assertIn('Training job is in progress. Cannot deploy',
                          response.get_json()['message'])

    # 4. Test cases for `check_deployment_status`
    def test_check_deployment_status_missing_job_id(self):
        with self.app.test_request_context('/check_deployment_status', method='GET', query_string={}):
            response, status_code = self.deploy_service.check_deployment_status(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # 5. Test cases for `increase_instance_count`
    def test_increase_instance_count_missing_instance_count(self):
        with self.app.test_request_context('/increase_instance_count', method='POST', json={'job_id': 'job-1234'}):
            response, status_code = self.deploy_service.increase_instance_count(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('instance_count is required',
                          response.get_json()['message'])

    # 6. Test cases for `delete_all_deployment_details`
    def test_delete_all_deployment_details_missing_job_id(self):
        with self.app.test_request_context('/delete_all_deployment_details', method='DELETE', query_string={}):
            response, status_code = self.deploy_service.delete_all_deployment_details(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # def test_delete_all_deployment_details_success(self):
    #     self.mock_dynamodb_client.get_item.return_value = {
    #         'Item': {'endpoint_name': {'S': 'endpoint-1234'}}}
    #     self.mock_sagemaker_client.delete_endpoint.return_value = {
    #         'ResponseMetadata': {'HTTPStatusCode': 200}}
    #     with self.app.test_request_context('/delete_all_deployment_details', method='DELETE', query_string={'job_id': 'job-1234'}):
    #         response, status_code = self.deploy_service.delete_all_deployment_details(
    #             request)
    #         self.assertEqual(status_code, 200)
    #         self.assertIn('Deployment details deleted successfully',
    #                       response.get_json()['message'])


if __name__ == '__main__':
    unittest.main()
