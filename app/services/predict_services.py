from botocore.config import Config as BotoConfig
from flask import jsonify
from botocore.exceptions import ClientError
import os
import numpy as np
import librosa
import boto3
import json


class PredictService:
    def __init__(self, s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn):
        self.s3_client = s3_client
        self.sagemaker_client = sagemaker_client
        self.dynamodb_client = dynamodb_client
        self.bucket_name = bucket_name
        self.role_arn = role_arn
        self.runtime_client = boto3.client('runtime.sagemaker')

    def predict(self, request):
        local_file_path = None  # Initialize local_file_path to None
        try:
            job_id = request.form.get('job_id')
            file_storage = request.files.get('file')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400
            if not file_storage:
                return jsonify({'status': 'fail', 'message': 'File is required'}), 400

            # Check if job_id exists and retrieve class labels
            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']
            endpoint_name = item.get('endpoint_name', {}).get('S')
            training_classes = item.get('training_classes', {}).get('SS', [])
            threshold = item.get('threshold', {}).get('N', 0.5)

            print('the threshold is', threshold)

            if not endpoint_name:
                return jsonify({'status': 'fail', 'message': 'Endpoint name not found'}), 404

            if not training_classes:
                return jsonify({'status': 'fail', 'message': 'Training classes not found'}), 400

            UPLOAD_FOLDER = 'uploads'
            # Save the uploaded file temporarily
            local_file_path = os.path.join(
                UPLOAD_FOLDER, file_storage.filename)
            file_storage.save(local_file_path)

            # Load the audio file and preprocess it
            audio, sr = librosa.load(local_file_path)
            melspec = np.mean(librosa.feature.melspectrogram(
                y=audio, sr=sr).T, axis=0)
            melspec = melspec.reshape(1, 16, 8, 1)

            # Predict using the endpoint
            response = self.runtime_client.invoke_endpoint(
                EndpointName=endpoint_name,
                ContentType='application/json',
                Body=json.dumps(melspec.tolist())
            )

            # Decode response
            result = response['Body'].read().decode('utf-8')
            prediction_data = json.loads(result)
            predicted_class = None

            # Obtain the index with the highest score for multiclass, or apply a threshold for binary
            if len(training_classes) == 2:
                pre = prediction_data['predictions'][0][0]
                threshold = float(threshold)
                print('pre', pre)
                print('threshold', threshold)

                prediction = int(
                    prediction_data['predictions'][0][0] > float(threshold))

                predicted_class = training_classes[prediction]
            else:
                # maximum prediction should be over the threshold, otherwise send unknown class
                prediction = np.argmax(prediction_data['predictions'][0])

                predicted_class = training_classes[prediction] if prediction_data['predictions'][0][prediction] > float(
                    threshold) else 'unknown'

            return jsonify({'status': 'success', 'prediction': predicted_class, 'prediction_data': prediction_data, 'training_classes': training_classes}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
        finally:
            if local_file_path and os.path.exists(local_file_path):
                os.remove(local_file_path)

    def add_threshold(self, request):
        try:
            job_id = request.json.get('job_id')
            threshold = request.json.get('threshold')
            threshold = float(threshold)

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            if not threshold:
                return jsonify({'status': 'fail', 'message': 'threshold is required'}), 400

            # threshold must be between 0 and 1
            if not 0 <= threshold <= 1:
                return jsonify({'status': 'fail', 'message': 'threshold must be between 0 and 1'}), 400

            # check the end point is in service
            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
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

            if endpoint_status != 'InService':
                return jsonify({'status': 'fail', 'message': 'Endpoint is not in service'}), 400

            # Update the job item in DynamoDB

            res = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='SET threshold = :threshold',
                ExpressionAttributeValues={':threshold': {'N': str(threshold)}}
            )

            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Threshold added successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to add threshold to DynamoDB'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def remove_threshold(self, request):
        try:
            job_id = request.args.get('job_id')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            # check the end point is in service
            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
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

            if endpoint_status != 'InService':
                return jsonify({'status': 'fail', 'message': 'Endpoint is not in service'}), 400

            # Update the job item in DynamoDB
            res = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='REMOVE threshold'
            )

            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Threshold removed successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to remove threshold from DynamoDB'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def approve(self, request):
        try:
            job_id = request.json.get('job_id')
            approve_name = request.json.get('approve_name')
            approve_date = request.json.get('approve_date')
            display_names_for_training_classes = request.json.get(
                'display_names_for_training_classes')

            if not job_id:

                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            if not approve_name:
                return jsonify({'status': 'fail', 'message': 'approve_name is required'}), 400

            if not approve_date:
                return jsonify({'status': 'fail', 'message': 'approve_date is required'}), 400

            if not display_names_for_training_classes:
                return jsonify({'status': 'fail', 'message': 'display_names_for_training_classes is required'}), 400

            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
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

            if endpoint_status != 'InService':
                return jsonify({'status': 'fail', 'message': 'Endpoint is not in service'}), 400

            # here check the order of the training classes and display_names_for_training_classes classes are in the same order, otherwise return error
            training_classes = item.get('training_classes', {}).get('SS', [])
            display_names = [x.get('class')
                             for x in display_names_for_training_classes]

            print('training_classes', training_classes)
            print('display_names', display_names)
            if training_classes != display_names:
                return jsonify({'status': 'fail', 'message': 'Training classes and display names do not match'}), 400

            # Update the job item in DynamoDB
            res = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='SET approve_name = :approve_name, approve_date = :approve_date, display_names_for_training_classes = :display_names_for_training_classes, approved = :approved',
                ExpressionAttributeValues={
                    ':approve_name': {'S': approve_name},
                    ':approve_date': {'S': approve_date},
                    ':display_names_for_training_classes': {
                        'L': [
                            {
                                'M': {
                                    'class': {'S': x['class']},
                                    'display_name': {'S': x['display_name']},
                                    'icon': {'S': x['icon']},
                                    'color': {'S': x['color']}
                                }
                            } for x in display_names_for_training_classes
                        ]
                    },
                    ':approved': {'BOOL': True}
                }
            )

            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Job approved successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to approve job'}), 500

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def reject(self, request):
        try:
            job_id = request.json.get('job_id')
            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400

            # check the end point is in service

            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
            )
            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']

            endpoint_name = item.get('endpoint_name', {}).get('S')

            if not endpoint_name:
                return jsonify({'status': 'fail', 'message': 'Endpoint name not found'}), 404

            response = self.sagemaker_client.describe_endpoint(EndpointName=endpoint_name
                                                               )

            endpoint_status = response.get('EndpointStatus', 'Unknown')

            if endpoint_status != 'InService':
                return jsonify({'status': 'fail', 'message': 'Endpoint is not in service'}), 400

            # remove threshold approvename date and make approvce to flase
            res = self.dynamodb_client.update_item(
                TableName='jobs',
                Key={'job_id': {'S': job_id}},
                UpdateExpression='REMOVE threshold, approve_name, approve_date, display_names_for_training_classes SET approved = :approved',
                ExpressionAttributeValues={':approved': {'BOOL': False}}
            )

            if res['ResponseMetadata']['HTTPStatusCode'] == 200:
                return jsonify({'status': 'success', 'message': 'Job rejected successfully'}), 200
            else:
                return jsonify({'status': 'fail', 'message': 'Failed to reject job'}), 500
        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_approved_jobs(self, request):
        # send approves jobs(for a one approve job job_id  and japproves_name )
        try:
            res = self.dynamodb_client.scan(
                TableName='jobs',
                FilterExpression='approved = :approved',
                ExpressionAttributeValues={':approved': {'BOOL': True}}
            )

            items = res.get('Items', [])
            jobs = []

            for item in items:
                job = {
                    'job_id': item.get('job_id', {}).get('S'),
                    'approve_name': item.get('approve_name', {}).get('S'),
                }
                jobs.append(job)

            return jsonify({'status': 'success', 'jobs': jobs}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def predict_with_display_names(self, request):
        local_file_path = None  # Initialize local_file_path to None
        try:
            job_id = request.form.get('job_id')
            file_storage = request.files.get('file')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400
            if not file_storage:
                return jsonify({'status': 'fail', 'message': 'File is required'}), 400

            # Check if job_id exists and retrieve class labels
            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']
            endpoint_name = item.get('endpoint_name', {}).get('S')
            training_classes = item.get('training_classes', {}).get('SS', [])
            threshold = item.get('threshold', {}).get('N', 0.5)
            display_names_for_training_classes = item.get(
                'display_names_for_training_classes', {}).get('L', [])

            print('the threshold is', threshold)

            if not endpoint_name:
                return jsonify({'status': 'fail', 'message': 'Endpoint name not found'}), 404

            if not training_classes:
                return jsonify({'status': 'fail', 'message': 'Training classes not found'}), 400

            if not display_names_for_training_classes:
                return jsonify({'status': 'fail', 'message': 'Display names for training classes not found'}), 400

            display_names_for_training_classes_formatted = [
                {
                    'class': dn.get('M', {}).get('class', {}).get('S', ''),
                    'display_name': dn.get('M', {}).get('display_name', {}).get('S', ''),
                    'icon': dn.get('M', {}).get('icon', {}).get('S', ''),
                    'color': dn.get('M', {}).get('color', {}).get('S', '')
                } for dn in display_names_for_training_classes
            ]

            print(display_names_for_training_classes_formatted)

            UPLOAD_FOLDER = 'uploads'
            # Save the uploaded file temporarily
            local_file_path = os.path.join(
                UPLOAD_FOLDER, file_storage.filename)
            file_storage.save(local_file_path)

            # Load the audio file and preprocess it
            audio, sr = librosa.load(local_file_path)
            melspec = np.mean(librosa.feature.melspectrogram(
                y=audio, sr=sr).T, axis=0)
            melspec = melspec.reshape(1, 16, 8, 1)

            # Predict using the endpoint
            response = self.runtime_client.invoke_endpoint(
                EndpointName=endpoint_name,
                ContentType='application/json',
                Body=json.dumps(melspec.tolist())
            )

            # Decode response
            result = response['Body'].read().decode('utf-8')
            prediction_data = json.loads(result)
            predicted_class = None
            probability = 0

            # Obtain the index with the highest score for multiclass, or apply a threshold for binary
            if len(training_classes) == 2:

                prediction = int(
                    prediction_data['predictions'][0][0] > float(threshold))

                predicted_class = training_classes[prediction]

                display_names_for_training_classes = display_names_for_training_classes_formatted[
                    prediction]

                if predicted_class == 'other':
                    probability = prediction_data['predictions'][0][0]
                else:
                    probability = 1-prediction_data['predictions'][0][0]

            else:
                # maximum prediction should be over the threshold, otherwise send unknown class
                prediction = np.argmax(prediction_data['predictions'][0])

                predicted_class = training_classes[prediction] if prediction_data['predictions'][0][prediction] > float(
                    threshold) else 'other'

                display_names_for_training_classes = display_names_for_training_classes_formatted[
                    prediction]

                # find the other class details from the display_names_for_training_classes
                if predicted_class == 'other':
                    for dn in display_names_for_training_classes_formatted:
                        if dn['class'] == 'other':
                            display_names_for_training_classes = dn
                            break

                probability = prediction_data['predictions'][0][prediction]

            return jsonify({'status': 'success', 'prediction': predicted_class, 'probability': probability, 'prediction_data': prediction_data, 'training_classes': training_classes, 'display': display_names_for_training_classes}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
        finally:
            if local_file_path and os.path.exists(local_file_path):
                os.remove(local_file_path)

    def predict_with_display_names_test(self, request):
        local_file_path = None  # Initialize local_file_path to None
        try:
            job_id = request.args.get('job_id')
            file_storage = request.files.get('file')

            if not job_id:
                return jsonify({'status': 'fail', 'message': 'job_id is required'}), 400
            if not file_storage:
                return jsonify({'status': 'fail', 'message': 'File is required'}), 400

            # Check if job_id exists and retrieve class labels
            res = self.dynamodb_client.get_item(
                TableName='jobs', Key={'job_id': {'S': job_id}}
            )

            if 'Item' not in res:
                return jsonify({'status': 'fail', 'message': 'job_id does not exist'}), 404

            item = res['Item']
            endpoint_name = item.get('endpoint_name', {}).get('S')
            training_classes = item.get('training_classes', {}).get('SS', [])
            threshold = item.get('threshold', {}).get('N', 0.5)
            display_names_for_training_classes = item.get(
                'display_names_for_training_classes', {}).get('L', [])

            print('the threshold is', threshold)

            if not endpoint_name:
                return jsonify({'status': 'fail', 'message': 'Endpoint name not found'}), 404

            if not training_classes:
                return jsonify({'status': 'fail', 'message': 'Training classes not found'}), 400

            if not display_names_for_training_classes:
                return jsonify({'status': 'fail', 'message': 'Display names for training classes not found'}), 400

            display_names_for_training_classes_formatted = [
                {
                    'class': dn.get('M', {}).get('class', {}).get('S', ''),
                    'display_name': dn.get('M', {}).get('display_name', {}).get('S', ''),
                    'icon': dn.get('M', {}).get('icon', {}).get('S', ''),
                    'color': dn.get('M', {}).get('color', {}).get('S', '')
                } for dn in display_names_for_training_classes
            ]

            print(display_names_for_training_classes_formatted)

            UPLOAD_FOLDER = 'uploads'
            # Save the uploaded file temporarily
            local_file_path = os.path.join(
                UPLOAD_FOLDER, file_storage.filename)
            file_storage.save(local_file_path)

            # Load the audio file and preprocess it
            audio, sr = librosa.load(local_file_path)
            melspec = np.mean(librosa.feature.melspectrogram(
                y=audio, sr=sr).T, axis=0)
            melspec = melspec.reshape(1, 16, 8, 1)

            # Predict using the endpoint
            response = self.runtime_client.invoke_endpoint(
                EndpointName=endpoint_name,
                ContentType='application/json',
                Body=json.dumps(melspec.tolist())
            )

            # Decode response
            result = response['Body'].read().decode('utf-8')
            prediction_data = json.loads(result)
            predicted_class = None
            probability = 0

            # Obtain the index with the highest score for multiclass, or apply a threshold for binary
            if len(training_classes) == 2:

                prediction = int(
                    prediction_data['predictions'][0][0] > float(threshold))

                predicted_class = training_classes[prediction]

                display_names_for_training_classes = display_names_for_training_classes_formatted[
                    prediction]

                if predicted_class == 'other':
                    probability = prediction_data['predictions'][0][0]
                else:
                    probability = 1-prediction_data['predictions'][0][0]

            else:
                # maximum prediction should be over the threshold, otherwise send unknown class
                prediction = np.argmax(prediction_data['predictions'][0])

                predicted_class = training_classes[prediction] if prediction_data['predictions'][0][prediction] > float(
                    threshold) else 'other'

                display_names_for_training_classes = display_names_for_training_classes_formatted[
                    prediction]

                # find the other class details from the display_names_for_training_classes
                if predicted_class == 'other':
                    for dn in display_names_for_training_classes_formatted:
                        if dn['class'] == 'other':
                            display_names_for_training_classes = dn
                            break

                probability = prediction_data['predictions'][0][prediction]

            # 'status': 'success',
            #     'prediction': predicted_class,
            #     'prediction_data': prediction_data,
            #     'training_classes': training_classes,
            #     'display_names_for_training_classes': display_names_for_training_classes,
            #     'probability': probability,
            #     'threshold': threshold

            return jsonify({'status': 'success', 'prediction': predicted_class, 'probability': probability, 'prediction_data': prediction_data, 'training_classes': training_classes, 'display': display_names_for_training_classes}), 200

        except ClientError as e:
            return jsonify({'status': 'fail', 'message': e.response['Error']['Message']}), 500
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
        finally:
            if local_file_path and os.path.exists(local_file_path):
                os.remove(local_file_path)
