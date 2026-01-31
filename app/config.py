import os


class Config:
    S3_BUCKET = os.getenv('S3_BUCKET')
    SECRET_KEY = os.getenv('SECRET_KEY')
    SAGEMAKER_ROLE_ARN = os.getenv('SAGEMAKER_ROLE_ARN')

    AWS_ACCESS_KEY_ID = os.getenv('aws_access_key_id')
    AWS_SECRET_ACCESS_KEY = os.getenv('aws_secret_access_key')
    TRAIN_IMAGE = os.getenv("TRAIN_IMAGE")
    PREPROCESS_IMAGE = os.getenv('PREPROCESS_IMAGE')
# TESTING = False


class TestConfig(Config):
    TESTING = True
