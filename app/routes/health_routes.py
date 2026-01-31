from flask import Blueprint, jsonify

bp = Blueprint('health_bp', __name__)

@bp.route('/', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy"}), 200
