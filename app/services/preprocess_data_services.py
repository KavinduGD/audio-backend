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


class PreprocessingService:
    def __init__(self, s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn, preprocess_image_uri):
        self.s3_client = s3_client
        self.sagemaker_client = sagemaker_client
        self.dynamodb_client = dynamodb_client
        self.bucket_name = bucket_name
        self.role_arn = role_arn
        self.docker_image_uri = preprocess_image_uri
        # self.docker_client = DockerClient.from_env()

    def get_next_job_id(self):
        try:
            # get all job ids
            response = self.dynamodb_client.scan(TableName='jobs',
                                                 ProjectionExpression='job_id')
            items = response['Items']
            existing_jobs = {item['job_id']['S'] for item in items}

            while True:
                numerical_character = random.choices(
                    string.digits, k=13)
                lowercase_character = random.choices(
                    string.ascii_lowercase, k=2)

                characters = list(numerical_character+lowercase_character)
                shuffle_characters = random.sample(characters, len(characters))
                job_id = ''.join(shuffle_characters)

                if job_id not in existing_jobs:
                    break

            return job_id, 200
        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_all_jobs_data(self):
        try:
            response = self.dynamodb_client.scan(TableName='jobs')
            items = response.get('Items', [])

            jobs = []
            for item in items:
                class_configs = item.get('class_configs', {}).get('L', [])
                class_configs_formatted = [
                    {
                        'class_name': config['M']['class_name']['S'],
                        'class_count': int(config['M']['class_count']['N']),
                        'type': config['M']['type']['S']
                    } for config in class_configs
                ]

                # Retrieve display_names_for_training_classes
                display_names_for_training_classes = item.get(
                    'display_names_for_training_classes', {}).get('L', [])
                display_names_for_training_classes_formatted = [
                    {
                        'class': dn.get('M', {}).get('class', {}).get('S', ''),
                        'display_name': dn.get('M', {}).get('display_name', {}).get('S', ''),
                        'icon': dn.get('M', {}).get('icon', {}).get('S', ''),
                        'color': dn.get('M', {}).get('color', {}).get('S', '')
                    } for dn in display_names_for_training_classes
                ]

                # Default job status
                job_status = 'Unknown'
                training_job_status = 'Unknown'
                endpoint_status = 'Unknown'

                # Retrieve the SageMaker job name
                sagemaker_job_name = item.get(
                    'sagemaker_preprocess_job_name', {}).get('S', '')

                sagemaker_train_job_name = item.get(
                    'sagemaker_train_job_name', {}).get('S', '')

                endpoint_name = item.get('endpoint_name', {}).get('S', '')

                if sagemaker_job_name:
                    try:
                        # Get the SageMaker job status
                        sm_response = self.sagemaker_client.describe_processing_job(
                            ProcessingJobName=sagemaker_job_name
                        )
                        job_status = sm_response.get(
                            'ProcessingJobStatus', 'Unknown')
                    except ClientError as e:
                        job_status = 'Error Retrieving Status'

                if sagemaker_train_job_name:
                    try:
                        # Get the SageMaker job status
                        sm_response = self.sagemaker_client.describe_training_job(
                            TrainingJobName=sagemaker_train_job_name
                        )
                        training_job_status = sm_response.get(
                            'TrainingJobStatus', 'Unknown')
                    except ClientError as e:
                        training_job_status = 'Error Retrieving Status'

                if endpoint_name:
                    try:
                        sm_response = self.sagemaker_client.describe_endpoint(
                            EndpointName=endpoint_name
                        )
                        endpoint_status = sm_response.get(
                            'EndpointStatus', 'Unknown')
                    except ClientError as e:
                        endpoint_status = 'Error Retrieving Status'

                # Retrieve hyperparameters
                hyperparameters = item.get('hyperparameters', {}).get('M', {})
                hyperparameters_formatted = {
                    key: value.get('S', '') for key, value in hyperparameters.items()
                }

                jobs.append({
                    'job_id': item['job_id']['S'],
                    'job_name': item['job_name']['S'],
                    'job_description': item['job_description']['S'],
                    'job_date': item['job_date']['S'],
                    'job_type': item['job_type']['S'],
                    'instance_type': item.get('instance_type', {}).get('S', ''),
                    'instance_count': item.get('instance_count', {}).get('N', 0),
                    'preprocess_date': item.get('preprocess_date', {}).get('S', ''),
                    'sagemaker_preprocess_job_name': sagemaker_job_name,
                    'class_configs': class_configs_formatted,
                    'preprocessing_job_status': job_status,
                    'train_architecture_type': item.get('train_architecture_type', {}).get('N', 0),
                    'train_instance_type': item.get('train_instance_type', {}).get('S', ''),
                    'train_instance_count': item.get('train_instance_count', {}).get('N', 0),
                    'train_date': item.get('train_date', {}).get('S', ''),
                    'training_classes': item.get('training_classes', {}).get('SS', []),
                    'accuracy': item.get('accuracy', {}).get('N', 0),
                    'hyperparameters': hyperparameters_formatted,
                    'classification_report': item.get('classification_report', {}).get('S', ''),
                    'sagemaker_train_job_name': sagemaker_train_job_name,
                    'training_job_status': training_job_status,
                    'deploy_instance_type': item.get('deploy_instance_type', {}).get('S', ''),
                    'deploy_instance_count': item.get('deploy_instance_count', {}).get('N', 0),
                    'deploy_date': item.get('deploy_date', {}).get('S', ''),
                    'endpoint_name': item.get('endpoint_name', {}).get('S', ''),
                    'endpoint_status': endpoint_status,
                    'threshold': item.get('threshold', {}).get('N', 0),
                    'approved': item.get('approved', {}).get('BOOL', False),
                    'approve_name': item.get('approve_name', {}).get('S', ''),
                    'approve_date': item.get('approve_date', {}).get('S', ''),
                    'display_names_for_training_classes': display_names_for_training_classes_formatted

                })

            return jsonify({'status': 'success', 'jobs': jobs}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_single_jobs_data(self, request):
        try:
            job_id = request.args.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            response = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in response:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = response['Item']
            class_configs = item.get('class_configs', {}).get('L', [])
            class_configs_formatted = [
                {
                    'class_name': config['M']['class_name']['S'],
                    'class_count': int(config['M']['class_count']['N']),
                    'type': config['M']['type']['S']
                } for config in class_configs
            ]
            job = {
                'job_id': item['job_id']['S'],
                'job_name': item['job_name']['S'],
                'job_description': item['job_description']['S'],
                'job_date': item['job_date']['S'],
                'job_type': item['job_type']['S'],
                'instance_type': item.get('instance_type', {}).get('S', ''),
                'instance_count': item.get('instance_count', {}).get('N', 0),
                'preprocess_date': item.get('preprocess_date', {}).get('S', ''),
                'sagemaker_job_name': item.get('sagemaker_job_name', {}).get('S', ''),
                'class_configs': class_configs_formatted
            }

            return jsonify({'status': 'success', 'job': job}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def preprocess_create(self, request):

        try:
            # Get job details from request body
            job_data = request.json
            job_id = job_data.get('job_id')
            job_name = job_data.get('job_name')
            job_description = job_data.get('job_description')
            job_date = job_data.get('job_date')
            job_type = job_data.get('job_type')

            if not all([job_id, job_name, job_description, job_date, job_type]):
                return jsonify({'status': 'fail', 'message': 'Missing required fields'}), 400

            # if job data has no 15 character and there must be 13 numbers and 2 lower case letter send a message
            if len(job_id) != 15:
                return jsonify({'status': 'fail', 'message': 'job_id must be 15 characters long'}), 400

            if not job_id.isalnum():
                return jsonify({'status': 'fail', 'message': 'job_id must be alphanumeric'}), 400

            if not job_id.islower():
                return jsonify({'status': 'fail', 'message': 'job_id must be lowercase'}), 400

            if job_type not in ['binary', 'multi']:
                return jsonify({'status': 'fail', 'message': 'job_type must be binary or multi'}), 400

            res = self.dynamodb_client.scan(
                TableName='jobs',
                FilterExpression='job_id = :job_id OR job_name = :job_name',
                ExpressionAttributeValues={
                    ':job_id': {'S': job_id},
                    ':job_name': {'S': job_name}
                }
            )

            if res['Items']:
                if any(item['job_id']['S'] == job_id for item in res['Items']):
                    return jsonify({'status': 'fail', 'message': 'job_id already exists'}), 400

                if any(item['job_name']['S'] == job_name for item in res['Items']):
                    return jsonify({'status': 'fail', 'message': 'job_name already exists'}), 400

            # Insert item into the DynamoDB table with correct type wrappers
            response = self.dynamodb_client.put_item(
                TableName='jobs',
                Item={
                    'job_id': {'S': job_id},
                    'job_name': {'S': job_name},
                    'job_description': {'S': job_description},
                    'job_date': {'S': job_date},
                    'job_type': {'S': job_type},
                }
            )

            return jsonify({'status': 'success', 'message': 'Job added successfully', 'response': response}), 201

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def update_basic_info(self, request):
        try:
            job_data = request.json
            job_id = job_data.get('job_id')
            job_name = job_data.get('job_name')
            job_description = job_data.get('job_description')
            job_type = job_data.get('job_type')
            modified_date = job_data.get('modified_date')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            if not modified_date:
                return jsonify({'status': 'fail', 'message': 'modified_date is required'}), 400

            if not any([job_name, job_description, job_type]):
                return jsonify({'status': 'fail', 'message': 'At least one of job_name, job_description, or job_type must be provided'}), 400

            if job_type and job_type not in ['binary', 'multi']:
                return jsonify({'status': 'fail', 'message': 'job_type must be binary or multi'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404
            # Check if job_name already exists
            if job_name:
                res = self.dynamodb_client.scan(
                    TableName='jobs',
                    FilterExpression='job_name = :job_name AND job_id <> :job_id',
                    ExpressionAttributeValues={
                        ':job_name': {'S': job_name},
                        ':job_id': {'S': job_id}
                    }
                )

                if res['Items']:
                    return jsonify({'status': 'fail', 'message': 'job_name already exists'}), 400

            update_expression = "SET"
            expression_attribute_values = {}
            if job_name:
                update_expression += " job_name = :job_name,"
                expression_attribute_values[':job_name'] = {'S': job_name}
            if job_description:
                update_expression += " job_description = :job_description,"
                expression_attribute_values[':job_description'] = {
                    'S': job_description}
            if job_type:
                update_expression += " job_type = :job_type,"
                expression_attribute_values[':job_type'] = {'S': job_type}
            if modified_date:
                update_expression += " job_date = :job_date,"
                expression_attribute_values[':job_date'] = {'S': modified_date}

            # Remove the trailing comma
            update_expression = update_expression.rstrip(',')

            # Update the item in the DynamoDB table
            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_attribute_values
            )

            return jsonify({'status': 'success', 'message': 'Job updated successfully', 'response': response}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def add_classes(self, request):
        try:
            # Get job details from the request body
            job_id = request.json.get('job_id')
            class_configs = request.json.get('class_configs')
            instance_type = request.json.get('instance_type')
            instance_count = request.json.get('instance_count')
            preprocess_date = request.json.get('preprocess_date')

            # Validate job_id
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            if not class_configs:
                return jsonify({'status': 'fail', 'message': 'class_configs is required'}), 400

            if not instance_type:
                return jsonify({'status': 'fail', 'message': 'instance_type is required'}), 400

            if not instance_count:
                return jsonify({'status': 'fail', 'message': 'instance_count is required'}), 400

            if not preprocess_date:
                return jsonify({'status': 'fail', 'message': 'preprocess_date is required'}), 400

            # Check if job_id exists
            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )
            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 400

                # Validate class_configs
            existing_class_names = set()
            for config in class_configs:
                class_name = config.get('class_name')
                class_count = config.get('class_count')

                if not class_name or not class_count or 'type' not in config:
                    return jsonify({'status': 'fail', 'message': 'Each class_config must contain class_name, class_count, and type'}), 400

                # Check for duplicate class names
                if class_name in existing_class_names:
                    return jsonify({'status': 'fail', 'message': f'Duplicate class_name found: {class_name}'}), 400
                existing_class_names.add(class_name)

                # Fetch total count from S3 for the class_name
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=f'input_data/{class_name}/')
                total_count = response['KeyCount']

                # Validate if class_count exceeds total count
                if int(class_count) > total_count:
                    return jsonify({'status': 'fail', 'message': f'class_count for {class_name} exceeds total available count of {total_count}'}), 400

            # Validate instance_type
            if instance_type not in instances:
                return jsonify({'status': 'fail', 'message': f'Invalid instance type. Must be one of {instances}'}), 400

            # Validate instance_count
            if int(instance_count) not in [1, 2, 3, 4, 5]:
                return jsonify({'status': 'fail', 'message': 'Instance count must be an integer between 1 and 5'}), 400

            # Update the item in the DynamoDB table
            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression="SET class_configs = :class_configs, instance_type = :instance_type, instance_count = :instance_count, preprocess_date = :preprocess_date",
                ExpressionAttributeValues={
                    ':class_configs': {
                        'L': [{
                            'M': {
                                'class_name': {'S': config['class_name']},
                                'class_count': {'N': str(config['class_count'])},
                                'type': {'S': config['type']}
                            }
                        } for config in class_configs]
                    },
                    ':instance_type': {'S': instance_type},
                    ':instance_count': {'N': str(instance_count)},
                    ':preprocess_date': {'S': preprocess_date}
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

    def delete_classes(self, request):
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

            # Delete the CSV file from S3
            csv_key = f'jobs/{job_name}/preprocessed_data/{job_name}_augmented_data.csv'
            self.s3_client.delete_object(Bucket=self.bucket_name, Key=csv_key)

            # Update the item in the DynamoDB table to remove class_configs and other attributes
            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression="REMOVE class_configs, instance_type, instance_count, preprocess_date, sagemaker_preprocess_job_name"
            )

            return jsonify({'status': 'success', 'message': 'Class configurations and related attributes removed successfully, CSV file deleted from S3'}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def preprocess_data_sagemaker(self, request):
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

            class_configs = res['Item'].get('class_configs', {}).get('L')

            if not class_configs:
                return jsonify({'status': 'fail', 'message': 'class_configs not found for the given job_id'}), 404

            # Convert class_configs from DynamoDB format to a list of dictionaries
            class_configs = [
                {
                    'class_name': config['M']['class_name']['S'],
                    'class_count': int(config['M']['class_count']['N']),
                    'type': config['M']['type']['S']
                }
                for config in class_configs
            ]

            if not class_configs:
                return jsonify({'status': 'fail', 'message': 'class_configs is required'}), 400

            instance_type = res['Item'].get(
                'instance_type', {}).get('S', 'ml.m5.xlarge')
            instance_count = res['Item'].get('instance_count', {}).get('N', 1)
            job_name = res['Item'].get('job_name', {}).get(
                'S', 'preprocess-data-job')

            processing_job_name = f'preprocess-data-job-{int(time.time())}'

            response = self.sagemaker_client.create_processing_job(
                ProcessingJobName=processing_job_name,
                AppSpecification={
                    'ImageUri': self.docker_image_uri,
                    'ContainerEntrypoint': ['python', '/opt/ml/processing/preprocess_data.py']
                },
                ProcessingInputs=[],
                ProcessingOutputConfig={
                    'Outputs': [
                        {
                            'OutputName': 'outputdata',
                            'S3Output': {
                                'S3Uri': f's3://{self.bucket_name}/{job_name}/preprocessed_data/',
                                'LocalPath': '/opt/ml/processing/output',
                                'S3UploadMode': 'EndOfJob'
                            }
                        }
                    ]
                },
                Environment={
                    'CLASS_CONFIGS': ','.join([f"{config['class_name']}:{config['class_count']}:{config['type']}" for config in class_configs]),
                    'S3_BUCKET': self.bucket_name,
                    'JOB_NAME': job_name,

                },
                RoleArn=self.role_arn,
                ProcessingResources={
                    'ClusterConfig': {
                        'InstanceType': instance_type,
                        'InstanceCount': int(instance_count),
                        'VolumeSizeInGB': 30
                    }
                }
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.dynamodb_client.update_item(
                    TableName='jobs',
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression="SET sagemaker_preprocess_job_name = :sagemaker_preprocess_job_name",
                    ExpressionAttributeValues={
                        ':sagemaker_preprocess_job_name': {'S': processing_job_name}
                    }
                )
                return jsonify({'status': 'success', 'message': "Preprocess job successfully started", "processing_job_name": processing_job_name}), 200
            else:
                return jsonify({'status': 'fail', 'message': "Preprocess job fail"}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def check_preprocess_job_status(self, request):
        try:
            job_id = request.args.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            # Retrieve the SageMaker job name from DynamoDB
            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            sagemaker_job_name = res['Item'].get(
                'sagemaker_preprocess_job_name', {}).get('S')
            if not sagemaker_job_name:
                return jsonify({'status': 'fail', 'message': 'sagemaker_job_name not found for the given job_id'}), 404

            # Get the SageMaker job status
            response = self.sagemaker_client.describe_processing_job(
                ProcessingJobName=sagemaker_job_name)

            job_status = response.get('ProcessingJobStatus', 'Unknown')

            time_to_complete = -1
            if job_status == 'Completed':
                start_time = response.get('ProcessingStartTime')
                end_time = response.get('ProcessingEndTime')
                if start_time and end_time:
                    time_to_complete = (end_time - start_time).total_seconds()

            return jsonify({
                'status': 'success',
                'ProcessingJobStatus': job_status,
                'TimeToComplete': time_to_complete

            }), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_csv_file(self, request):
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
            sagemaker_job_name = res['Item'].get(
                'sagemaker_preprocess_job_name', {}).get('S')

            if not sagemaker_job_name:
                return jsonify({'status': 'fail', 'message': 'No preprocess job associated with this job_id'}), 404

            # Get the SageMaker job status
            response = self.sagemaker_client.describe_processing_job(
                ProcessingJobName=sagemaker_job_name)

            job_status = response.get('ProcessingJobStatus', 'Unknown')
            if job_status != 'Completed':
                return jsonify({'status': 'fail', 'message': 'Preprocessing job is not completed or has failed'}), 400

            # Construct the S3 key for the CSV file
            csv_key = f'jobs/{job_name}/preprocessed_data/{job_name}_augmented_data.csv'

            # Generate a presigned URL for the CSV file
            presigned_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.bucket_name, 'Key': csv_key},
                ExpiresIn=3600  # URL expiration time in seconds
            )

            return jsonify({'status': 'success', 'presigned_url': presigned_url}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_whole_job(self, request):
        # there will be job_id in request body in a array so that all of them will be deleted
        try:
            job_ids = request.json.get('job_ids')
            if not job_ids:
                return jsonify({'status': 'fail', 'message': 'job_ids is required'}), 400

            for job_id in job_ids:
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
                sagemaker_job_name = res['Item'].get(
                    'sagemaker_preprocess_job_name', {}).get('S')
                if sagemaker_job_name:
                    # Get the SageMaker job status
                    response = self.sagemaker_client.describe_processing_job(
                        ProcessingJobName=sagemaker_job_name)

                    job_status = response.get('ProcessingJobStatus', 'Unknown')
                    if job_status == 'InProgress':
                        return jsonify({'status': 'fail', 'message': 'Preprocessing job is in progress. Cannot delete job'}), 400

                    # Delete the CSV file from S3
                    csv_key = f'jobs/{job_name}/preprocessed_data/{job_name}_augmented_data.csv'
                    self.s3_client.delete_object(
                        Bucket=self.bucket_name, Key=csv_key)

                # Delete the item from the DynamoDB table
                self.dynamodb_client.delete_item(
                    TableName='jobs',
                    Key={'job_id': {'S': job_id}}
                )

            return jsonify({'status': 'success', 'message': 'Job deleted successfully'}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    # def preprocess_data_local(self, request):
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

    #         class_configs = res['Item'].get('class_configs', {}).get('L')

    #         if not class_configs:
    #             return jsonify({'status': 'fail', 'message': 'class_configs not found for the given job_id'}), 404

    #         # Convert class_configs from DynamoDB format to a list of dictionaries
    #         class_configs = [
    #             {
    #                 'class_name': config['M']['class_name']['S'],
    #                 'class_count': int(config['M']['class_count']['N']),
    #                 'type': config['M']['type']['S']
    #             }
    #             for config in class_configs
    #         ]

    #         if not class_configs:
    #             return jsonify({'status': 'fail', 'message': 'class_configs is required'}), 400

    #         job_name = res['Item'].get('job_name', {}).get(
    #             'S', 'preprocess-data-job')

    #         # Run Docker container with the specified environment variables
    #         container = self.docker_client.containers.run(
    #             'sagemaker-audio-processing:latest',
    #             environment={
    #                 'CLASS_CONFIGS': ','.join([f"{config['class_name']}:{config['class_count']}:{config['type']}" for config in class_configs]),
    #                 'S3_BUCKET': self.bucket_name,
    #                 'AWS_ACCESS_KEY_ID': current_app.config['AWS_ACCESS_KEY_ID'],
    #                 'AWS_SECRET_ACCESS_KEY': current_app.config['AWS_SECRET_ACCESS_KEY'],
    #                 'JOB_NAME': job_name
    #             },
    #             volumes={
    #                 '/opt/ml/processing/output': {'bind': '/opt/ml/processing/output', 'mode': 'rw'}
    #             },
    #             detach=True
    #         )

    #         # Capture and print logs for debugging
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

    # auto ad classes
    def auto_add_classes(self, request):
        try:
            job_id = request.json.get('job_id')
            instance_type = request.json.get('instance_type')
            instance_count = request.json.get('instance_count')
            preprocess_date = request.json.get('preprocess_date')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400
            if not instance_type:
                return jsonify({'status': 'fail', 'message': 'instance_type is required'}), 400
            if not instance_count:
                return jsonify({'status': 'fail', 'message': 'instance_count is required'}), 400
            if not preprocess_date:
                return jsonify({'status': 'fail', 'message': 'preprocess_date is required'}), 400

            # Get job_type from DynamoDB
            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )
            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 400
            job_type = res['Item'].get('job_type', {}).get('S', 'multi')

            # List classes from S3 input_data/
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix='input_data/',
                Delimiter='/'
            )
            class_configs = []
            for prefix in response.get('CommonPrefixes', []):
                class_name = prefix['Prefix'].split('/')[-2]
                # Count files in each class folder
                files_resp = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=f'input_data/{class_name}/'
                )
                class_count = files_resp.get('KeyCount', 0)
                if class_count > 0:
                    class_configs.append({
                        'class_name': class_name,
                        'class_count': 400,
                        'type': 'main'
                    })

            if not class_configs:
                return jsonify({'status': 'fail', 'message': 'No classes found in input_data/'}), 400

            # Reuse add_classes logic for validation and DynamoDB update
            request_data = {
                'job_id': job_id,
                'class_configs': class_configs,
                'instance_type': instance_type,
                'instance_count': instance_count,
                'preprocess_date': preprocess_date
            }
            # Call add_classes directly
            return self.add_classes(type('obj', (object,), {'json': request_data})())

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
