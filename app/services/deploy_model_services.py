from botocore.config import Config as BotoConfig
from flask import current_app, request, jsonify
import time
from botocore.exceptions import ClientError
from sagemaker.tensorflow import TensorFlowModel

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
    'ml.g5.12xlarge', 'ml.g5.16xlarge', 'ml.g5.24xlarge', 'ml.g5.48xlarge', 'ml.t2.medium']


class DeployingService:
    def __init__(self, s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn):
        self.s3_client = s3_client
        self.sagemaker_client = sagemaker_client
        self.dynamodb_client = dynamodb_client
        self.bucket_name = bucket_name
        self.role_arn = role_arn

    def add_deployment_details(self, request):
        try:
            job_id = request.json.get('job_id')
            deploy_instance_type = request.json.get('deploy_instance_type')
            deploy_instance_count = request.json.get('deploy_instance_count')
            deploy__date = request.json.get('deploy_date')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            if not deploy_instance_type:
                return jsonify({'status': 'fail', 'message': 'Instance type is required'}), 400

            if not deploy_instance_count:
                return jsonify({'status': 'fail', 'message': 'Instance count is required'}), 400

            if not deploy__date:
                return jsonify({'status': 'fail', 'message': 'Date is required'}), 400

            if deploy_instance_type not in instances:
                return jsonify({'status': 'fail', 'message': 'Instance type not supported'}), 400

            if int(deploy_instance_count) not in [1, 2, 3, 4, 5]:
                return jsonify({'status': 'fail', 'message': 'Instance count must be between 1 and 5'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='SET deploy_instance_type = :deploy_instance_type, deploy_instance_count = :deploy_instance_count, deploy_date = :deploy_date',
                ExpressionAttributeValues={
                    ':deploy_instance_type': {'S': deploy_instance_type},
                    ':deploy_instance_count': {'N': str(deploy_instance_count)},
                    ':deploy_date': {'S': deploy__date}

                }
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Deployment details added successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to add deployment details'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_deployment_details(self, request):
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

            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='REMOVE deploy_instance_type, deploy_instance_count, deploy_date'
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Deployment details deleted successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to delete deployment details'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def deploy_model(self, request):
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
                return jsonify({'status': 'fail', 'message': 'Training job is in progress. Cannot deploy'}), 400

            deploy_instance_type = item.get(
                'deploy_instance_type', {}).get('S')
            deploy_instance_count = item.get(
                'deploy_instance_count', {}).get('N')

            if not deploy_instance_type:
                return jsonify({'status': 'fail', 'message': 'Instance type is required'}), 400

            if not deploy_instance_count:
                return jsonify({'status': 'fail', 'message': 'Instance count is required'}), 400
            # Path to your .h5 model within the S3 bucket
            model_dir = f'jobs/{job_name}/train_artifacts/model.tar.gz'

            # Full S3 model path
            model_s3_path = f's3://{self.bucket_name}/{model_dir}'

            # Create TensorFlowModel object
            model = TensorFlowModel(
                model_data=model_s3_path,
                role=self.role_arn,
                framework_version='2.16'
            )

            # Deploy the model as an endpoint
            predictor = model.deploy(
                initial_instance_count=int(deploy_instance_count),
                instance_type=deploy_instance_type,
                wait=False
            )

            if predictor.endpoint_name:

                response = self.dynamodb_client.update_item(
                    TableName='jobs',
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression='SET endpoint_name = :endpoint_name',
                    ExpressionAttributeValues={
                        ':endpoint_name': {'S': predictor.endpoint_name}
                    }
                )
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    return jsonify({'status': 'success', 'EndpointName': predictor.endpoint_name}), 200
                else:
                    return jsonify({'status': 'fail', 'message': 'Failed to add endpoint name to DynamoDB'}), 500

            else:
                return jsonify({'status': 'fail', 'message': 'Failed to deploy model'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def check_deployment_status(self, request):
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
            endpoint_name = item.get('endpoint_name', {}).get('S')

            if not endpoint_name:
                return jsonify({'status': 'fail', 'message': 'Endpoint name not found'}), 404

            response = self.sagemaker_client.describe_endpoint(
                EndpointName=endpoint_name
            )

            endpoint_status = response.get('EndpointStatus', 'Unknown')

            return jsonify({'status': 'success', 'EndpointStatus': endpoint_status}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def increase_instance_count(self, request):
        try:
            job_id = request.json.get('job_id')
            instance_count = request.json.get('instance_count')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            if not instance_count:
                return jsonify({'status': 'fail', 'message': 'instance_count is required'}), 400

            if int(instance_count) not in [1, 2, 3, 4, 5]:
                return jsonify({'status': 'fail', 'message': 'Instance count must be between 1 and 5'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']

            endpoint_name = item.get('endpoint_name', {}).get('S')

            if not endpoint_name:
                return jsonify({'status': 'fail', 'message': 'Endpoint name not found'}), 404

            # Describe the current endpoint
            response = self.sagemaker_client.describe_endpoint(
                EndpointName=endpoint_name)
            endpoint_status = response.get('EndpointStatus', 'Unknown')

            if endpoint_status != 'InService':
                return jsonify({'status': 'fail', 'message': 'Endpoint is not in service'}), 400

            # Describe the current endpoint configuration
            endpoint_config_name = response.get('EndpointConfigName')

            current_endpoint_config = self.sagemaker_client.describe_endpoint_config(
                EndpointConfigName=endpoint_config_name)

            # Create a new EndpointConfig with the updated instance count
            production_variants = current_endpoint_config['ProductionVariants']
            for variant in production_variants:
                variant['InitialInstanceCount'] = int(
                    instance_count)  # Update the instance count

            new_endpoint_config_name = f"{endpoint_name}-config-{int(time.time())}"

            self.sagemaker_client.create_endpoint_config(
                EndpointConfigName=new_endpoint_config_name,
                ProductionVariants=production_variants
            )

            # Update the endpoint to use the new configuration
            response = self.sagemaker_client.update_endpoint(
                EndpointName=endpoint_name,
                EndpointConfigName=new_endpoint_config_name
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                # Update the deploy_instance_count in DynamoDB
                response = self.dynamodb_client.update_item(
                    TableName='jobs',
                    Key={'job_id': {'S': job_id}},
                    UpdateExpression='SET deploy_instance_count = :deploy_instance_count',
                    ExpressionAttributeValues={
                        ':deploy_instance_count': {'N': str(instance_count)}
                    }
                )
                if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                    return jsonify({'status': 'success', 'message': 'Instance count updated successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to update instance count'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_all_deployment_details(self, request):
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

            # stop the endpoint
            item = res['Item']
            endpoint_name = item.get('endpoint_name', {}).get('S')

            if endpoint_name:
                self.sagemaker_client.delete_endpoint(
                    EndpointName=endpoint_name
                )

            response = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='REMOVE deploy_instance_type, deploy_instance_count, deploy_date, endpoint_name'
            )

            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Deployment details deleted successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to delete deployment details'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
