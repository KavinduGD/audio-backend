from flask import Blueprint, request, jsonify, current_app
from ..services.preprocess_data_services import PreprocessingService
import boto3


bp = Blueprint('preprocess_data_routes', __name__)


s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')
dynamodb_client = boto3.client('dynamodb')


@bp.before_request
def before_request():
    global preprocess_service
    bucket_name = current_app.config['S3_BUCKET']
    role_arn = current_app.config['SAGEMAKER_ROLE_ARN']
    preprocess_image_uri = current_app.config['PREPROCESS_IMAGE']
    preprocess_service = PreprocessingService(
        s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn, preprocess_image_uri)


@bp.route('/get-next-job-id', methods=['GET'])
def get_next_job_id():
    return preprocess_service.get_next_job_id()


@bp.route('/get-all-jobs-data', methods=['GET'])
def get_all_jobs_data():
    return preprocess_service.get_all_jobs_data()


@bp.route('/get-single-jobs-data', methods=['GET'])
def get_single_jobs_data():
    return preprocess_service.get_single_jobs_data(request)


@bp.route('/preprocess-create', methods=['POST'])
def preprocess_create():
    return preprocess_service.preprocess_create(request)


@bp.route('/update-basic-info', methods=['PATCH'])
def update_basic_info():
    return preprocess_service.update_basic_info(request)


@bp.route('/add-classes', methods=['PATCH'])
def add_classes():
    return preprocess_service.add_classes(request)


@bp.route('/delete-classes', methods=['Delete'])
def delete_classes():
    return preprocess_service.delete_classes(request)


@bp.route('/preprocess-sagemaker', methods=['POST'])
def preprocess_data_sagemker():
    return preprocess_service.preprocess_data_sagemaker(request)


@bp.route('/check-preprocess-job-status', methods=['GET'])
def check_preprocess_job_status():
    return preprocess_service.check_preprocess_job_status(request)


@bp.route('/get-csv-file', methods=['GET'])
def get_csv_file():
    return preprocess_service.get_csv_file(request)


@bp.route('/delete-whole-job', methods=['DELETE'])
def delete_whole_job():
    return preprocess_service.delete_whole_job(request)

# auto add classes


@bp.route('/auto_add_classes', methods=['POST'])
def auto_add_classes():
    return preprocess_service.auto_add_classes(request)

# @bp.route('/preprocess-local', methods=['POST'])
# def preprocess_data_local():
#     return preprocess_service.preprocess_data_local(request)
