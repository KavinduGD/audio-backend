from flask import Blueprint, request, jsonify, current_app
import boto3
from ..services.input_data_services import DataService
from botocore.config import Config as BotoConfig

bp = Blueprint('input_data_routes', __name__)
# Setup S3 client with Transfer Acceleration
s3_config = BotoConfig(s3={'use_accelerate_endpoint': True})
s3_client = boto3.client('s3', config=s3_config)


@bp.before_request
def before_request():
    global data_service
    bucket_name = current_app.config['S3_BUCKET']
    data_service = DataService(s3_client, bucket_name)


@bp.route('/compare', methods=['POST'])
def compare():
    return data_service.compare(request)


@bp.route('/rename', methods=['POST'])
def rename():
    return data_service.rename(request)


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


@bp.route('/copy-and-keep-both-with-percentage', methods=['POST'])
def copy_and_keep_both_with_percentage():
    return data_service.copy_and_keep_both_with_percentage(request)


@bp.route('/copy-and-override-with-percentage', methods=['POST'])
def copy_and_override_with_percentage():
    return data_service.copy_and_override_with_percentage(request)


@bp.route('/delete-all-audios-from-set-of-classes', methods=['DELETE'])
def delete_all_audios_from_set_of_classes():
    return data_service.delete_all_audios_from_set_of_classes(request)
