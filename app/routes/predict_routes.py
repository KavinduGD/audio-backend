from flask import Blueprint, request, jsonify, current_app
from ..services.predict_services import PredictService
import boto3


bp = Blueprint('predict_routes', __name__)


s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')
dynamodb_client = boto3.client('dynamodb')


@bp.before_request
def before_request():
    global predict_service
    bucket_name = current_app.config['S3_BUCKET']
    role_arn = current_app.config['SAGEMAKER_ROLE_ARN']
    predict_service = PredictService(
        s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn)


@bp.route('/predict', methods=['POST'])
def predict():
    return predict_service.predict(request)


@bp.route('/add-threshold', methods=['POST'])
def add_threshold():
    return predict_service.add_threshold(request)


@bp.route('/remove-threshold', methods=['DELETE'])
def remove_threshold():
    return predict_service.remove_threshold(request)


@bp.route('/approve', methods=['POST'])
def approve():
    return predict_service.approve(request)


@bp.route('/reject', methods=['POST'])
def reject():
    return predict_service.reject(request)


@bp.route('/get_approved_jobs', methods=['GET'])
def get_approved_jobs():
    return predict_service.get_approved_jobs(request)


@bp.route('/predict-with-display-names', methods=['POST'])
def predict_with_display_names():
    return predict_service.predict_with_display_names(request)


@bp.route('/predict-test', methods=['POST'])
def predict_with_display_names_test():
    return predict_service.predict_with_display_names_test(request)
