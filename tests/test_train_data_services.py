import unittest
from unittest.mock import MagicMock, patch
from flask import Flask, request
from app.services.train_data_services import TrainingService


class TestTrainingService(unittest.TestCase):

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
        cls.train_service = TrainingService(
            cls.mock_s3_client, cls.mock_sagemaker_client, cls.mock_dynamodb_client, cls.bucket_name, cls.role_arn)

    # 1. Test cases for `add_train_details`
    def test_add_train_details_missing_params(self):
        with self.app.test_request_context('/add-train-details', method='POST', json={}):
            response, status_code = self.train_service.add_train_details(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('Job ID is required', response.get_json()[
                          'message'])  # Adjusted to expected message

    # def test_add_train_details_success(self):
    #     self.mock_dynamodb_client.update_item.return_value = {
    #         'ResponseMetadata': {'HTTPStatusCode': 200}}
    #     with self.app.test_request_context('/add-train-details', method='POST', json={
    #         'job_id': 'job-1234', 'train_architecture_type': 'type1', 'train_instance_type': 'ml.m5.large',
    #         'train_instance_count': 1
    #     }):
    #         response, status_code = self.train_service.add_train_details(
    #             request)
    #         self.assertEqual(status_code, 200)
    #         self.assertIn('Class configurations and instance settings updated successfully',
    #                       response.get_json()['message'])

    # 2. Test cases for `delete_train_details`
    def test_delete_train_details_missing_job_id(self):
        with self.app.test_request_context('/delete-train-details', method='DELETE', query_string={}):
            response, status_code = self.train_service.delete_train_details(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('Job ID is required', response.get_json()['message'])

    # 3. Test cases for `train_model_local`
    def test_train_model_local_missing_job_id(self):
        with self.app.test_request_context('/train-model-local', method='POST', json={}):
            response, status_code = self.train_service.train_model_local(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # def test_train_model_local_success(self):
    #     self.mock_dynamodb_client.get_item.return_value = {
    #         'Item': {'train_architecture_type': {'N': '1'}, 'job_name': {'S': 'test_job'}, 'job_type': {'S': 'classification'}}
    #     }
    #     with patch('docker.from_env') as mock_docker:
    #         mock_container = MagicMock()
    #         mock_container.logs.return_value = b"Container logs"
    #         mock_container.wait.return_value = {"StatusCode": 0}
    #         mock_docker.return_value.containers.run.return_value = mock_container

    #         with self.app.test_request_context('/train-model-local', method='POST', json={'job_id': 'test_job_id'}):
    #             with self.app.app_context():
    #                 response, status_code = self.train_service.train_model_local(
    #                     request)
    #                 self.assertEqual(status_code, 201)
    #                 self.assertIn(
    #                     'Preprocessing job completed successfully.', response.get_json()['message'])

    # 4. Test cases for `train_model_sagemaker`
    def test_train_model_sagemaker_missing_job_id(self):
        with self.app.test_request_context('/train-model-sagemaker', method='POST', json={}):
            response, status_code = self.train_service.train_model_sagemaker(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # 5. Test cases for `check_train_job_status`
    def test_check_train_job_status_missing_job_id(self):
        with self.app.test_request_context('/check-train-job-status', method='GET', query_string={}):
            response, status_code = self.train_service.check_train_job_status(
                request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    def test_check_train_job_status_success(self):
        self.mock_dynamodb_client.get_item.return_value = {
            'Item': {'sagemaker_train_job_name': {'S': 'sagemaker-job'}}
        }
        self.mock_sagemaker_client.describe_training_job.return_value = {
            'TrainingJobStatus': 'Completed', 'TrainingTimeInSeconds': 3600
        }
        with self.app.test_request_context('/check-train-job-status', method='GET', query_string={'job_id': 'test_job_id'}):
            with self.app.app_context():
                response, status_code = self.train_service.check_train_job_status(
                    request)
                self.assertEqual(status_code, 200)
                self.assertEqual(response.get_json()[
                                 'TrainingJobStatus'], 'Completed')

    # 6. Test cases for `get_plot_images`
    def test_get_plot_images_missing_job_id(self):
        with self.app.test_request_context('/get_plot_images', method='GET', query_string={}):
            response, status_code = self.train_service.get_plot_images(request)
            self.assertEqual(status_code, 400)
            self.assertIn('job_id is required', response.get_json()['message'])

    # def test_get_plot_images_success(self):
    #     self.mock_dynamodb_client.get_item.return_value = {'Item': {
    #         'job_name': {'S': 'test_job'}, 'sagemaker_train_job_name': {'S': 'train_job'}}}
    #     self.mock_sagemaker_client.describe_training_job.return_value = {
    #         'TrainingJobStatus': 'Completed'}
    #     self.mock_s3_client.generate_presigned_url.return_value = 'http://example.com/presigned_url'

    #     with self.app.test_request_context('/get_plot_images', method='GET', query_string={'job_id': 'test_job_id'}):
    #         with self.app.app_context():
    #             response, status_code = self.train_service.get_plot_images(
    #                 request)
    #             self.assertEqual(status_code, 200)
    #             self.assertIn('accuracy_plot_light_url',
    #                           response.get_json()['plots'])


if __name__ == '__main__':
    unittest.main()
