from docker import DockerClient
from botocore.config import Config as BotoConfig
from flask import current_app, request, jsonify
import time
from botocore.exceptions import ClientError
import random
import string

instances = [
    "ml.r5d.large", "ml.r5d.xlarge", "ml.r5d.2xlarge", "ml.r5d.4xlarge", "ml.r5d.8xlarge", "ml.r5d.12xlarge", "ml.r5d.16xlarge", "ml.r5d.24xlarge",
    "ml.m5.large", "ml.m5.xlarge", "ml.m5.2xlarge", "ml.m5.4xlarge", "ml.m5.12xlarge", "ml.m5.24xlarge", "ml.m4.xlarge", "ml.m4.2xlarge",
    "ml.c5.xlarge", "ml.c5.2xlarge", "ml.c5.4xlarge", "ml.c5.9xlarge", "ml.c5.18xlarge", "ml.c4.xlarge", "ml.c4.2xlarge", 'ml.g4dn.4xlarge', 'ml.g4dn.8xlarge', 'ml.g4dn.12xlarge', 'ml.g4dn.16xlarge',
    'ml.g5.xlarge', 'ml.g5.2xlarge', 'ml.g5.4xlarge', 'ml.g5.8xlarge',
    'ml.m4.2xlarge', 'ml.m4.4xlarge', 'ml.m4.10xlarge', 'ml.m4.16xlarge',
    'ml.t3.medium', 'ml.t3.large', 'ml.t3.xlarge', 'ml.t3.2xlarge',
    'ml.r5.large', 'ml.r5.xlarge', 'ml.r5.2xlarge', 'ml.r5.4xlarge',
    'ml.r5.8xlarge', 'ml.r5.12xlarge', 'ml.r5.16xlarge', 'ml.r5.24xlarge',
    'ml.p2.xlarge', 'ml.p2.8xlarge', 'ml.p2.16xlarge', 'ml.p3.2xlarge',
    'ml.p3.8xlarge', 'ml.p3.16xlarge', 'ml.g4dn.xlarge', 'ml.g4dn.2xlarge',
    'ml.g5.12xlarge', 'ml.g5.16xlarge', 'ml.g5.24xlarge', 'ml.g5.48xlarge']


class TrainingService:
    def __init__(self, s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn, train_image_uri):
        self.s3_client = s3_client
        self.sagemaker_client = sagemaker_client
        self.dynamodb_client = dynamodb_client
        self.bucket_name = bucket_name
        self.role_arn = role_arn
        self.docker_image_uri = train_image_uri
        # self.docker_client = DockerClient.from_env()

    def add_train_details(self, request):
        try:
            job_id = request.json.get('job_id')
            train_architecture_type = request.json.get(
                'train_architecture_type')
            train_instance_type = request.json.get('train_instance_type')
            train_instance_count = request.json.get('train_instance_count')
            train__date = request.json.get('train_date')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'Job ID is required'}), 400

            if not train_architecture_type:
                return jsonify({'status': 'fail', 'message': 'Architecture type is required'}), 400

            if not train_instance_type:
                return jsonify({'status': 'fail', 'message': 'Instance type is required'}), 400

            if not train_instance_count:
                return jsonify({'status': 'fail', 'message': 'Instance count is required'}), 400

            if not train__date:
                return jsonify({'status': 'fail', 'message': 'Date is required'}), 400

            if train_instance_type not in instances:
                return jsonify({'status': 'fail', 'message': 'Instance type not supported'}), 400

            if int(train_instance_count) not in [1, 2, 3, 4, 5]:
                return jsonify({'status': 'fail', 'message': 'Instance count must be an integer between 1 and 5'}), 400
            # Check if job_id exists
            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )
            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 400

            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='SET train_architecture_type = :train_architecture_type, train_instance_type = :train_instance_type, train_instance_count = :train_instance_count, train_date = :train_date',
                ExpressionAttributeValues={
                    ':train_architecture_type': {'N': str(train_architecture_type)},
                    ':train_instance_type': {'S': train_instance_type},
                    ':train_instance_count': {'N': str(train_instance_count)},
                    ':train_date': {'S': train__date}
                }
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Class configurations and instance settings updated successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Could not update class configurations and instance settings', 'response': response}), 400

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_train_details(self, request):
        try:
            job_id = request.args.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'Job ID is required'}), 400

            # Check if job_id exists
            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )
            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 400

            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='REMOVE train_architecture_type, train_instance_type, train_instance_count, train_date'
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Class configurations and instance settings deleted successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Could not delete class configurations and instance settings', 'response': response}), 400

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def train_model_sagemaker(self, request):
        try:
            job_id = request.json.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']
            train_architecture_type = item.get(
                'train_architecture_type', {}).get('N')
            job_name = item.get('job_name', {}).get('S')
            job_type = item.get('job_type', {}).get('S')
            train_instance_type = item.get(
                'train_instance_type', {}).get('S', 'ml.m5.xlarge')
            train_instance_count = item.get(
                'train_instance_count', {}).get('N', 1)

            if not train_architecture_type:
                return jsonify({'status': 'fail', 'message': 'train_architecture_type not found'}), 404

            if not job_name:
                return jsonify({'status': 'fail', 'message': 'job_name not found'}), 404

            if not job_type:
                return jsonify({'status': 'fail', 'message': 'job_type not found'}), 404

            training_job_name = f'training-job-{int(time.time())}'

            response = self.sagemaker_client.create_training_job(
                TrainingJobName=training_job_name,

                AlgorithmSpecification={
                    'TrainingImage': self.docker_image_uri,
                    'TrainingInputMode': 'File'
                },
                RoleArn=self.role_arn,
                InputDataConfig=[
                    {
                        'ChannelName': 'training',
                        'DataSource': {
                            'S3DataSource': {
                                'S3DataType': 'S3Prefix',
                                'S3Uri': f's3://{self.bucket_name}/jobs/{job_name}/preprocessed_data/',
                                'S3DataDistributionType': 'FullyReplicated'
                            }
                        },
                        'ContentType': 'text/csv',
                        'InputMode': 'File'
                    }
                ],
                OutputDataConfig={
                    'S3OutputPath': f's3://{self.bucket_name}/jobs/{job_name}/train_artifacts/'
                },
                ResourceConfig={
                    'InstanceType': train_instance_type,
                    'InstanceCount': int(train_instance_count),
                    'VolumeSizeInGB': 50
                },
                Environment={
                    'S3_BUCKET': self.bucket_name,
                    'AWS_ACCESS_KEY_ID': current_app.config['AWS_ACCESS_KEY_ID'],
                    'AWS_SECRET_ACCESS_KEY': current_app.config['AWS_SECRET_ACCESS_KEY'],
                    'JOB_ID': job_id,
                    'JOB_NAME': job_name,
                    'JOB_TYPE': job_type,
                    "AWS_REGION": "ap-south-1",
                    'TRAIN_ARCHITECTURE_TYPE': train_architecture_type
                },
                StoppingCondition={
                    'MaxRuntimeInSeconds': 43200
                }
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.dynamodb_client.update_item(
                    TableName='jobs',
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression="SET sagemaker_train_job_name = :sagemaker_train_job_name",
                    ExpressionAttributeValues={
                        ':sagemaker_train_job_name': {'S': training_job_name}
                    }
                )
                return jsonify({'status': 'success', 'message': "Preprocess job successfully started", "training_job_name": training_job_name}), 200
            else:
                return jsonify({'status': 'fail', 'message': "Preprocess job fail"}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def check_train_job_status(self, request):
        try:
            job_id = request.args.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']
            sagemaker_train_job_name = item.get(
                'sagemaker_train_job_name', {}).get('S')

            if not sagemaker_train_job_name:
                return jsonify({'status': 'fail', 'message': 'sagemaker_train_job_name not found'}), 404

            response = self.sagemaker_client.describe_training_job(
                TrainingJobName=sagemaker_train_job_name
            )
            job_status = response.get('TrainingJobStatus', 'Unknown')

            time_to_complete = -1
            if job_status == 'Completed':
                time_to_complete = response['TrainingTimeInSeconds']

            return jsonify({'status': 'success', 'TrainingJobStatus': job_status, 'TimeToComplete': time_to_complete}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_all_train_data(self, request):
        try:
            job_id = request.args.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']
            sagemaker_train_job_name = item.get(
                'sagemaker_train_job_name', {}).get('S')
            job_name = item.get('job_name', {}).get('S')

            if not sagemaker_train_job_name:
                return jsonify({'status': 'fail', 'message': 'sagemaker_train_job_name not found'}), 404

            response = self.sagemaker_client.describe_training_job(
                TrainingJobName=sagemaker_train_job_name
            )

            job_status = response.get('TrainingJobStatus', 'Unknown')

            if job_status == 'InProgress':
                return jsonify({'status': 'fail', 'message': 'Training job is in progress'}), 400

            train_artifacts = f'jobs/{job_name}/train_artifacts/'

            # delete all files in the train_artifacts folder
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for result in paginator.paginate(Bucket=self.bucket_name, Prefix=train_artifacts):
                if 'Contents' in result:
                    for key in result['Contents']:
                        self.s3_client.delete_object(
                            Bucket=self.bucket_name, Key=key['Key'])

            self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression="REMOVE sagemaker_train_job_name, train_architecture_type, train_instance_type, train_instance_count, train_date, training_classes, classification_report, accuracy, hyperparameters"
            )

            return jsonify({'status': 'success', 'message': 'Training job deleted successfully'}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_plot_images(self, request):
        try:
         # Get job_id from request
            job_id = request.args.get('job_id')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            # Check if job_id exists
            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            # Retrieve job_name
            job_name = res['Item'].get('job_name', {}).get('S')
            if not job_name:
                return jsonify({'status': 'fail', 'message': 'job_name not found'}), 404

            # Retrieve SageMaker job name
            sagemaker_train_job_name = res['Item'].get(
                'sagemaker_train_job_name', {}).get('S')

            if not sagemaker_train_job_name:
                return jsonify({'status': 'fail', 'message': 'sagemaker_train_job_name not found'}), 404

            response = self.sagemaker_client.describe_training_job(
                TrainingJobName=sagemaker_train_job_name
            )

            job_status = response.get('TrainingJobStatus', 'Unknown')
            if job_status != 'Completed':
                return jsonify({'status': 'fail', 'message': 'Training job is not completed'}), 400

            time_to_complete = -1
            if job_status == 'Completed':
                time_to_complete = response['TrainingTimeInSeconds']

            accuracy_plot_light_key = f'jobs/{job_name}/train_artifacts/accuracy_plot_light.png'
            accuracy_plot_dark_key = f'jobs/{job_name}/train_artifacts/accuracy_plot_dark.png'
            loss_plot_light_key = f'jobs/{job_name}/train_artifacts/loss_plot_light.png'
            loss_plot_dark_key = f'jobs/{job_name}/train_artifacts/loss_plot_dark.png'
            confusion_matrix_plot_light_key = f'jobs/{job_name}/train_artifacts/confusion_matrix_light.png'
            confusion_matrix_plot_dark_key = f'jobs/{job_name}/train_artifacts/confusion_matrix_dark.png'

            # generate persigned url for the images
            accuracy_plot_light_url = self.s3_client.generate_presigned_url('get_object', Params={
                'Bucket': self.bucket_name, 'Key': accuracy_plot_light_key}, ExpiresIn=3600)
            accuracy_plot_light_url = accuracy_plot_light_url

            accuracy_plot_dark_url = self.s3_client.generate_presigned_url('get_object', Params={
                'Bucket': self.bucket_name, 'Key': accuracy_plot_dark_key}, ExpiresIn=3600)
            accuracy_plot_dark_url = accuracy_plot_dark_url

            loss_plot_light_url = self.s3_client.generate_presigned_url('get_object', Params={
                'Bucket': self.bucket_name, 'Key': loss_plot_light_key}, ExpiresIn=3600)
            loss_plot_light_url = loss_plot_light_url

            loss_plot_dark_url = self.s3_client.generate_presigned_url('get_object', Params={
                'Bucket': self.bucket_name, 'Key': loss_plot_dark_key}, ExpiresIn=3600)
            loss_plot_dark_url = loss_plot_dark_url

            confusion_matrix_plot_light_url = self.s3_client.generate_presigned_url('get_object', Params={
                'Bucket': self.bucket_name, 'Key': confusion_matrix_plot_light_key}, ExpiresIn=3600)
            confusion_matrix_plot_light_url = confusion_matrix_plot_light_url

            confusion_matrix_plot_dark_url = self.s3_client.generate_presigned_url('get_object', Params={
                'Bucket': self.bucket_name, 'Key': confusion_matrix_plot_dark_key}, ExpiresIn=3600)
            confusion_matrix_plot_dark_url = confusion_matrix_plot_dark_url

            return jsonify({'status': 'success', 'plots': {'accuracy_plot_light_url': accuracy_plot_light_url, 'accuracy_plot_dark_url': accuracy_plot_dark_url, 'loss_plot_light_url': loss_plot_light_url, 'loss_plot_dark_url': loss_plot_dark_url, 'confusion_matrix_plot_light_url': confusion_matrix_plot_light_url, 'confusion_matrix_plot_dark_url': confusion_matrix_plot_dark_url}, 'time_to_complete': time_to_complete}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    # def train_model_local(self, request):
    #     try:
    #         job_id = request.json.get('job_id')
    #         if not job_id:
    #             return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

    #         res = self.dynamodb_client.get_item(
    #             TableName='jobs',
    #             Key={'job_id': {'S': job_id}}
    #         )

    #         if 'Item' not in res:
    #             return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

    #         item = res['Item']
    #         train_architecture_type = item.get(
    #             'train_architecture_type', {}).get('N')
    #         job_name = item.get('job_name', {}).get('S')
    #         job_type = item.get('job_type', {}).get('S')

    #         if not train_architecture_type:
    #             return jsonify({'status': 'fail', 'message': 'train_architecture_type not found'}), 404

    #         if not job_name:
    #             return jsonify({'status': 'fail', 'message': 'job_name not found'}), 404

    #         if not job_type:
    #             return jsonify({'status': 'fail', 'message': 'job_type not found'}), 404

    #         container = self.docker_client.containers.run(
    #             'sagemaker-audio-train:latest',
    #             environment={
    #                 'S3_BUCKET': self.bucket_name,
    #                 'AWS_ACCESS_KEY_ID': current_app.config['AWS_ACCESS_KEY_ID'],
    #                 'AWS_SECRET_ACCESS_KEY': current_app.config['AWS_SECRET_ACCESS_KEY'],
    #                 'JOB_ID': job_id,
    #                 'JOB_NAME': job_name,
    #                 'JOB_TYPE': job_type,
    #                 "AWS_REGION": "ap-south-1",
    #                 'TRAIN_ARCHITECTURE_TYPE': train_architecture_type
    #             },

    #             detach=True
    #         )

    #         for line in container.logs(stream=True):
    #             print(line.strip())

    #         result = container.wait()

    #         if result["StatusCode"] != 0:
    #             logs = container.logs().decode("utf-8")
    #             container.remove()
    #             raise Exception(f"Container failed with logs: {logs}")
    #         else:
    #             logs = container.logs().decode("utf-8")
    #             container.remove()
    #             return jsonify({'status': 'success', 'message': 'Preprocessing job completed successfully.'}), 201

    #     except Exception as e:
    #         return jsonify({'status': 'fail', 'message': str(e)}), 500
