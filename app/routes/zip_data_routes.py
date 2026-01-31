from flask import Blueprint, request, jsonify, current_app
import boto3
from ..services.zip_data_services import DataService
from botocore.config import Config as BotoConfig

bp = Blueprint('zip_data_routes', __name__)
# Setup S3 client with Transfer Acceleration
s3_config = BotoConfig(s3={'use_accelerate_endpoint': True})
s3_client = boto3.client('s3', config=s3_config)



@bp.before_request
def before_request():
    global data_service
    bucket_name = current_app.config['S3_BUCKET']
    data_service = DataService(s3_client, bucket_name)


# @bp.route('/get-all-s3')
# def get_all_s3():
#     return data_service.get_all_s3()


@bp.route('/upload-zip-slow', methods=['POST'])
def upload_zip_slow():
    return data_service.upload_zip_slow(request)


@bp.route('/upload-zip-fast', methods=['POST'])
def upload_zip_fast():
    return data_service.upload_zip_fast(request)


@bp.route('/class-count', methods=['POST'])
def get_class_count():
    return data_service.get_class_count(request)


@bp.route('/get-all-class-count', methods=['GET'])
def get_all_class_names_and_data_points():
    return data_service.get_all_class_names_and_data_points()


@bp.route('/get-all-audios', methods=['POST'])
def get_all_audios():
    return data_service.get_all_audios(request)


@bp.route('/get-random-audios', methods=['POST'])
def get_random_audios():
    return data_service.get_random_audios(request)


@bp.route('/delete-all-audios-from-set-of-classes', methods=['DELETE'])
def delete_all_audios_from_set_of_classes():
    return data_service.delete_all_audios_from_set_of_classes(request)


@bp.route('/delete-all-class-audios', methods=['DELETE'])
def delete_all_class_audios():
    return data_service.delete_all_class_audios(request)


@bp.route('/delete-percentage-audios', methods=['DELETE'])
def delete_percentage_audios():
    return data_service.delete_percentage_audios(request)
