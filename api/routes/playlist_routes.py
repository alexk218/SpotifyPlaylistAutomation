from flask import Blueprint, request, jsonify, current_app

bp = Blueprint('playlists', __name__, url_prefix='/api/playlists')