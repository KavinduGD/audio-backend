import zipfile
import boto3
from flask import jsonify
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from botocore.config import Config as BotoConfig
import io
import random
import datetime


class DataService:
    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    def compare(self, request):
        try:
            zip_class_name = request.json.get('zip_class_name')
            input_class_name = request.json.get('input_class_name')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': "zip_class_name and input_class_name are required"}), 400

        if not zip_class_name or not input_class_name:
            return jsonify({'status': 'fail', 'message': "zip_class_name and input_class_name are required"}), 400

        try:
            # List all objects in the zip_class_name
            response_zip = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{zip_class_name}/')

            zip_audio_files = {obj['Key'].split(
                '/')[-1] for obj in response_zip.get('Contents', []) if obj['Key'].endswith('.wav')}

            # List all objects in the input_class_name
            response_input = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{input_class_name}/')

            input_audio_files = {obj['Key'].split(
                '/')[-1] for obj in response_input.get('Contents', []) if obj['Key'].endswith('.wav')}

            # Find common audio files
            common_audio_files = zip_audio_files.intersection(
                input_audio_files)

            if not common_audio_files:
                common_audio_files = list(common_audio_files)
                return jsonify({'status': 'success', 'common_audio_files': common_audio_files}), 200

            # Convert set to list
            common_audio_files = list(common_audio_files)
            return jsonify({'status': 'success', 'common_audio_files': common_audio_files}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def rename(self, request):
        try:
            zip_class_name = request.json.get('zip_class_name')
            input_class_name = request.json.get('input_class_name')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': "zip_class_name and input_class_name are required"}), 400

        if not zip_class_name or not input_class_name:
            return jsonify({'status': 'fail', 'message': "zip_class_name and input_class_name are required"}), 400

        try:
            # List all objects in the zip_class_name
            response_zip = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{zip_class_name}/')

            zip_audio_files = {obj['Key']: obj['Key'].split(
                '/')[-1] for obj in response_zip.get('Contents', []) if obj['Key'].endswith('.wav')}

            # List all objects in the input_class_name
            response_input = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{input_class_name}/')

            input_audio_files = {obj['Key'].split(
                '/')[-1] for obj in response_input.get('Contents', []) if obj['Key'].endswith('.wav')}

            # Find common audio files
            common_audio_files = {key: filename for key, filename in zip_audio_files.items(
            ) if filename in input_audio_files}

            renamed_files = []
            for key, filename in common_audio_files.items():
                # Generate new filename with a timestamp suffix
                new_filename = f"{filename.rsplit('.', 1)[0]}_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.wav"
                new_key = f'zip_data/{zip_class_name}/{new_filename}'

                # Copy and then delete the old object
                self.s3_client.copy_object(Bucket=self.bucket_name, CopySource={
                    'Bucket': self.bucket_name, 'Key': key}, Key=new_key)
                self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)

                renamed_files.append({
                    'old_filename': key,
                    'new_filename': new_key
                })

            return jsonify({'status': 'success', 'renamed_files': renamed_files}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_class_count(self, request):
        try:
            class_name = request.json.get('class_name')
            if not class_name:
                return jsonify({'status': 'fail', 'message': 'There are no class_name key in the request body'}), 400
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 500

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{class_name}/')
            count = response['KeyCount']
            return jsonify({'class_name': class_name, 'count': count}), 200
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_all_class_names_and_data_points(self):
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix='input_data/', Delimiter='/')

            class_names = []
            for prefix in response.get('CommonPrefixes', []):
                class_name = prefix.get('Prefix').split(
                    '/')[1]  # Get class name from the prefix
                class_names.append(class_name)

            # Get data points for each class
            class_data_points = {}
            for class_name in class_names:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=f'input_data/{class_name}/')
                data_points = response['KeyCount']
                class_data_points[class_name] = data_points

            return jsonify({'status': 'success', 'classes': class_data_points}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_all_audios(self, request):
        try:
            class_name = request.json.get('class_name')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 400

        if not class_name:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 400

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{class_name}/')

            audio_files = []
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.wav'):
                    presigned_url = self.s3_client.generate_presigned_url(
                        'get_object',
                        Params={'Bucket': self.bucket_name, 'Key': obj['Key']},
                        ExpiresIn=3600)  # URL expires in 1 hour
                    name = obj['Key'].split('/')[-1]
                    last_modified = obj['LastModified']
                    size = obj['Size']
                    audio_files.append({
                        'name': name,
                        'url': presigned_url,
                        'last_modified': last_modified,
                        'size': size
                    })

            if not audio_files:
                return jsonify({'status': 'fail', 'message': 'No audio files found for the specified class_name'}), 404

            return jsonify({'status': 'success', 'audio_files': audio_files}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_random_audios(self, request):
        try:
            class_name = request.json.get('class_name')
            count = request.json.get('count')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 400

        if not class_name or not count:
            return jsonify({'status': 'fail', 'message': 'class name and count is required'}), 400

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{class_name}/')

            audio_files = [obj['Key'] for obj in response.get(
                'Contents', []) if obj['Key'].endswith('.wav')]

            if not audio_files:
                return jsonify({'status': 'fail', 'message': 'No audio files found for the specified class_name'}), 404

            if len(audio_files) <= count:
                selected_files = audio_files
            else:
                selected_files = random.sample(audio_files, count)

            audio_urls = []
            for file_key in selected_files:
                presigned_url = self.s3_client.generate_presigned_url(
                    'get_object',
                    Params={'Bucket': self.bucket_name, 'Key': file_key},
                    ExpiresIn=3600  # URL expires in 1 hour
                )
                audio_urls.append(presigned_url)

            return jsonify({'status': 'success', 'audio_files': audio_urls}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def copy_and_keep_both_with_percentage(self, request):
        try:
            zip_class_name = request.json.get('zip_class_name')
            input_class_name = request.json.get('input_class_name')
            percentage = request.json.get('percentage')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'zip_class_name, input_class_name and percentage are required'}), 400

        if not zip_class_name or not input_class_name or not percentage or not isinstance(percentage, (int, float)):
            return jsonify({'status': 'fail', 'message': 'zip_class_name, input_class_name and valid percentage are required'}), 400

        try:
            # List all objects in the zip_class_name
            response_zip = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{zip_class_name}/')

            zip_audio_files = {obj['Key']: obj['Key'].split(
                '/')[-1] for obj in response_zip.get('Contents', []) if obj['Key'].endswith('.wav')}

            # Calculate the number of files to copy
            total_files = len(zip_audio_files)
            num_files_to_copy = int((percentage / 100) * total_files)

            if num_files_to_copy <= 0:
                return jsonify({'status': 'fail', 'message': 'Percentage too low, no files to copy'}), 400

            selected_files = random.sample(
                list(zip_audio_files.items()), num_files_to_copy)

            for key, filename in selected_files:
                new_key = f'input_data/{input_class_name}/{filename}'
                self.s3_client.copy_object(Bucket=self.bucket_name, CopySource={
                    'Bucket': self.bucket_name, 'Key': key}, Key=new_key)

            response_after_copy = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{input_class_name}/')
            total_files_after_copy = response_after_copy['KeyCount']

            return jsonify({'status': 'success', 'total_files_after_copy': total_files_after_copy}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def copy_and_override_with_percentage(self, request):
        try:
            zip_class_name = request.json.get('zip_class_name')
            input_class_name = request.json.get('input_class_name')
            percentage = request.json.get('percentage')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'zip_class_name, input_class_name and percentage are required'}), 400

        if not zip_class_name or not input_class_name or not percentage or not isinstance(percentage, (int, float)):
            return jsonify({'status': 'fail', 'message': 'zip_class_name, input_class_name and valid percentage are required'}), 400

        try:
            # List all objects in the zip_class_name
            response_zip = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{zip_class_name}/')

            zip_audio_files = {obj['Key']: obj['Key'].split(
                '/')[-1] for obj in response_zip.get('Contents', []) if obj['Key'].endswith('.wav')}

            # Calculate the number of files to copy
            total_files = len(zip_audio_files)
            num_files_to_copy = int((percentage / 100) * total_files)

            if num_files_to_copy <= 0:
                return jsonify({'status': 'fail', 'message': 'Percentage too low, no files to copy'}), 400

            for key, filename in random.sample(list(zip_audio_files.items()), num_files_to_copy):
                new_key = f'input_data/{input_class_name}/{filename}'
                self.s3_client.copy_object(Bucket=self.bucket_name, CopySource={
                    'Bucket': self.bucket_name, 'Key': key}, Key=new_key)

            response_after_copy = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'input_data/{input_class_name}/')
            total_files_after_copy = response_after_copy['KeyCount']

            return jsonify({'status': 'success', 'total_files_after_copy': total_files_after_copy}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_all_audios_from_set_of_classes(self, request):
        try:
            class_names = request.json.get('class_names')

            if not class_names or not isinstance(class_names, list):
                return jsonify({'status': 'fail', 'message': 'A list of class names is required'}), 400

            deleted_classes = []
            not_found_classes = []

            for class_name in class_names:
                # List all objects in the specified class
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=f'input_data/{class_name}/')

                keys_to_delete = [{'Key': obj['Key']}
                                  for obj in response.get('Contents', [])]

                if not keys_to_delete:
                    not_found_classes.append(class_name)
                    continue

                # Delete objects in batches of 1000 (maximum allowed by S3 in a single delete request)
                while keys_to_delete:
                    batch = keys_to_delete[:1000]
                    self.s3_client.delete_objects(
                        Bucket=self.bucket_name,
                        Delete={'Objects': batch}
                    )
                    keys_to_delete = keys_to_delete[1000:]

                deleted_classes.append(class_name)

            if not deleted_classes and not not_found_classes:
                return jsonify({'status': 'fail', 'message': 'No classes found to delete'}), 404

            return jsonify({
                'status': 'success',
                'deleted_classes': deleted_classes,
                'not_found_classes': not_found_classes
            }), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
