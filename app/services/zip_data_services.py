import zipfile
import boto3
from flask import jsonify
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from botocore.config import Config as BotoConfig
import io
import random


class DataService:
    def __init__(self, s3_client, bucket_name):
        self.s3_client = s3_client
        self.bucket_name = bucket_name

    # def upload_zip_slow(self, request):
    #     # zip file
    #     if 'file' not in request.files:
    #         return jsonify({'status': 'fail', 'message': 'There are no file key in the request body'}), 400
    #     file = request.files['file']

    #     # class_name
    #     class_name = request.form.get('class_name')
    #     if not class_name:
    #         return jsonify({'status': 'fail', 'message': 'There are no class_name key in the request body'}), 400

    #     try:
    #         if file and zipfile.is_zipfile(file):
    #             # check zip file name is sounds.zip
    #             if file.filename != 'sounds.zip':
    #                 return jsonify({'status': 'fail', 'message': 'The uploaded file name should be sounds.zip'}), 400

    #             # check the structure of the zip file
    #             with zipfile.ZipFile(file) as z:
    #                 if len(z.namelist()) == 1:
    #                     return jsonify({'status': 'fail', 'message': 'No files in the zip file'}), 400

    #                 for index, filename in enumerate(z.namelist()):
    #                     if len(filename.split('/')) != 2:
    #                         return jsonify({'status': 'fail', 'message': f'Invalid file structure: {filename}'}), 400
    #                     if not filename.endswith('.wav') and index != 0:
    #                         return jsonify({'status': 'fail', 'message': f'Invalid file type in zip: {filename}'}), 400

    #             with zipfile.ZipFile(file) as z:
    #                 count = 0
    #                 for filename in z.namelist():
    #                     if len(filename.split('/')) == 2 and filename.split('/')[1] != "":
    #                         key_file_name = filename.split('/')[1]
    #                         s3_key = f'zip_data/{class_name}/{key_file_name}'

    #                         # Upload the extracted file to S3
    #                         with z.open(filename) as extracted_file:
    #                             print(count)
    #                             count += 1
    #                             self.s3_client.upload_fileobj(
    #                                 extracted_file, self.bucket_name, s3_key)
    #             return jsonify({'status': 'success', 'message': 'All files uploaded successfully'}), 201
    #         else:
    #             return jsonify({'status': 'fail', 'message': 'The uploaded file is not a zip file'}), 400
    #     except Exception as e:
    #         print(e)
    #         return str(e), 500

    def upload_zip_fast(self, request):
        # Zip file validation
        if "file" not in request.files:
            return jsonify({"status": "fail", "message": "There are no file key in the request body"}), 400
        file = request.files["file"]

        # Class name validation
        class_name = request.form.get("class_name")
        if not class_name:
            return jsonify({"status": "fail", "message": "There are no class_name key in the request body"}), 400

        try:
            if file and zipfile.is_zipfile(file):
                # Check zip file name is sounds.zip
                if file.filename != "sounds.zip":
                    return jsonify({"status": "fail", "message": "The uploaded file name should be sounds.zip"}), 400

                # Load zipfile contents in the main thread
                z = zipfile.ZipFile(file)

                if len(z.namelist()) == 1:
                    return jsonify({"status": "fail", "message": "No files in the zip file"}), 400

                for index, filename in enumerate(z.namelist()):
                    if len(filename.split("/")) != 2:
                        return jsonify({"status": "fail", "message": f"Invalid file structure: {filename}"}), 400
                    if not filename.endswith(".wav") and index != 0:
                        return jsonify({"status": "fail", "message": f"Invalid file type in zip: {filename}"}), 400

                # Use ThreadPoolExecutor to upload files concurrently
                upload_count = 0

                def upload_file_to_s3(s3_client, bucket_name, class_name, zip_file_bytes, filename):
                    nonlocal upload_count
                    try:
                        key_file_name = filename.split("/")[1]
                        s3_key = f"zip_data/{class_name}/{key_file_name}"

                        with zipfile.ZipFile(io.BytesIO(zip_file_bytes)) as z:
                            with z.open(filename) as extracted_file:
                                s3_client.upload_fileobj(
                                    extracted_file, bucket_name, s3_key)
                                # Increment count on successful upload
                                upload_count += 1
                                print(upload_count)
                    except Exception as e:
                        print(f"Error uploading {filename}: {e}")

                # Read the zip file into a bytes object
                file.seek(0)  # Reset file pointer to the beginning
                zip_file_bytes = file.read()

                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = []

                    for filename in z.namelist():
                        if len(filename.split("/")) == 2 and filename.split("/")[1] != "":
                            futures.append(executor.submit(
                                upload_file_to_s3, self.s3_client, self.bucket_name, class_name, zip_file_bytes, filename))

                    # Wait for all uploads to complete
                    [future.result() for future in futures]

                # Fetch the total count of audio files for the class
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')

                total_count = sum(1 for obj in response.get(
                    'Contents', []) if obj['Key'].endswith('.wav'))

                return jsonify({
                    "status": "success",
                    "message": f"All files uploaded successfully. {upload_count} files were uploaded.",
                    'uploaded_count': upload_count,
                    'total_count': total_count  # Add the total count of audio files for the class
                }), 201
            else:
                return jsonify({"status": "fail", "message": "The uploaded file is not a zip file"}), 400
        except Exception as e:
            print(e)
            return str(e), 500

    def get_class_count(self, request):
        try:
            class_name = request.json.get('class_name')
            if not class_name:
                return jsonify({'status': 'fail', 'message': 'There are no class_name key in the request body'}), 400
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 500

        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')
            count = response['KeyCount']
            return jsonify({'class_name': class_name, 'count': count}), 200
        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def get_all_class_names_and_data_points(self):
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix='zip_data/', Delimiter='/')

            class_names = []
            for prefix in response.get('CommonPrefixes', []):
                class_name = prefix.get('Prefix').split(
                    '/')[1]  # Get class name from the prefix
                class_names.append(class_name)

            # Get data points for each class
            class_data_points = {}
            for class_name in class_names:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')
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
                Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')

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
                Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')

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
                    Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')

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

    def delete_all_class_audios(self, request):
        try:
            class_name = request.json.get('class_name')

        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 500

        if not class_name:
            return jsonify({'status': 'fail', 'message': 'class name is required'}), 400
        try:
            # List all objects in the specified class
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')

            keys_to_delete = [{'Key': obj['Key']}
                              for obj in response.get('Contents', [])]

            if not keys_to_delete:
                return jsonify({'status': 'fail', 'message': 'No files found for the specified class_name'}), 404

            # Delete objects in batches of 1000 (maximum allowed by S3 in a single delete request)
            while keys_to_delete:
                batch = keys_to_delete[:1000]
                self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': batch}
                )
                keys_to_delete = keys_to_delete[1000:]

            return jsonify({'status': 'success', 'message': f'All files in class {class_name} deleted successfully'}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500

    def delete_percentage_audios(self, request):
        try:
            class_name = request.json.get('class_name')
            percentage = request.json.get('percentage')
        except Exception as e:
            return jsonify({'status': 'fail', 'message': 'class name and percentage are required'}), 400

        if not class_name or not percentage or not isinstance(percentage, (int, float)):
            return jsonify({'status': 'fail', 'message': 'class name and valid percentage are required'}), 400

        try:
            # List all objects in the specified class
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name, Prefix=f'zip_data/{class_name}/')

            audio_files = [obj['Key'] for obj in response.get(
                'Contents', []) if obj['Key'].endswith('.wav')]

            if not audio_files:
                return jsonify({'status': 'fail', 'message': 'No audio files found for the specified class_name'}), 404

            total_files = len(audio_files)
            num_files_to_delete = int((percentage / 100.0) * total_files)

            if num_files_to_delete <= 0:
                return jsonify({'status': 'fail', 'message': 'Percentage too low, no files to delete'}), 400

            selected_files = random.sample(audio_files, num_files_to_delete)
            keys_to_delete = [{'Key': file_key} for file_key in selected_files]

            # Delete objects in batches of 1000 (maximum allowed by S3 in a single delete request)
            while keys_to_delete:
                batch = keys_to_delete[:1000]
                self.s3_client.delete_objects(
                    Bucket=self.bucket_name,
                    Delete={'Objects': batch}
                )
                keys_to_delete = keys_to_delete[1000:]

            return jsonify({'status': 'success', 'message': f'{num_files_to_delete} files in class {class_name} deleted successfully'}), 200

        except Exception as e:
            return jsonify({'status': 'fail', 'message': str(e)}), 500
