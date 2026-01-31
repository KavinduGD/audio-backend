from flask import Blueprint, request, jsonify, current_app
from ..services.train_data_services import TrainingService
import boto3


bp = Blueprint('train_data_routes', __name__)


s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')
dynamodb_client = boto3.client('dynamodb')


@bp.before_request
def before_request():
    global train_service
    bucket_name = current_app.config['S3_BUCKET']
    role_arn = current_app.config['SAGEMAKER_ROLE_ARN']
    train_image_uri = current_app.config['TRAIN_IMAGE']
    train_service = TrainingService(
        s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn, train_image_uri)


@bp.route('/add-train-details', methods=['POST'])
def add_train_details():
    return train_service.add_train_details(request)


@bp.route('/delete-train-details', methods=['DELETE'])
def delete_train_details():
    return train_service.delete_train_details(request)


@bp.route('/train-model-local', methods=['POST'])
def train_model_local():
    return train_service.train_model_local(request)


@bp.route('/train-model-sagemaker', methods=['POST'])
def train_model_sagemaker():
    return train_service.train_model_sagemaker(request)


@bp.route('/check-train-job-status', methods=['GET'])
def check_train_job_status():
    return train_service.check_train_job_status(request)


@bp.route('/delete_all_train_data', methods=['DELETE'])
def delete_all_train_data():
    return train_service.delete_all_train_data(request)


@bp.route('/get_plot_images', methods=['GET'])
def get_plot_images():
    return train_service.get_plot_images(request)
