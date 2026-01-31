from flask import Blueprint, request, jsonify, current_app
from ..services.deploy_model_services import DeployingService
import boto3


bp = Blueprint('deploy_model_routes', __name__)


s3_client = boto3.client('s3')
sagemaker_client = boto3.client('sagemaker')
dynamodb_client = boto3.client('dynamodb')


@bp.before_request
def before_request():
    global deploy_service
    bucket_name = current_app.config['S3_BUCKET']
    role_arn = current_app.config['SAGEMAKER_ROLE_ARN']
    deploy_service = DeployingService(
        s3_client, sagemaker_client, dynamodb_client, bucket_name, role_arn)


@bp.route('/add_deployment_details', methods=['POST'])
def add_deployment_details():
    return deploy_service.add_deployment_details(request)


@bp.route('/delete_deployment_details', methods=['DELETE'])
def delete_deployment_details():
    return deploy_service.delete_deployment_details(request)


@bp.route('/deploy_model', methods=['POST'])
def deploy_model():
    return deploy_service.deploy_model(request)


@bp.route('/check_deployment_status', methods=['GET'])
def check_deployment_status():
    return deploy_service.check_deployment_status(request)


@bp.route('/increase_instance_count', methods=['POST'])
def increase_instance_count():
    return deploy_service.increase_instance_count(request)


@bp.route('delete_all_deployment_details', methods=['DELETE'])
def delete_all_deployment_details():
    return deploy_service.delete_all_deployment_details(request)
