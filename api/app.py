from flask import Flask
from flask_cors import CORS
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def create_app(test_config=None):
    """Create and configure the Flask app."""
    app = Flask(__name__)
    CORS(app, origins=["https://xpui.app.spotify.com", "https://open.spotify.com", "http://localhost:4000", "*"])
    app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

    # Get environment variables
    app.config['MASTER_TRACKS_DIRECTORY_SSD'] = os.getenv("MASTER_TRACKS_DIRECTORY_SSD")
    app.config['MASTER_PLAYLIST_ID'] = os.getenv("MASTER_PLAYLIST_ID")

    # Apply test config if provided
    if test_config:
        app.config.update(test_config)

    # Import and register blueprints
    from api.routes import track_routes, playlist_routes, validation_routes, sync_routes, rekordbox_routes

    app.register_blueprint(track_routes.bp)
    app.register_blueprint(playlist_routes.bp)
    app.register_blueprint(validation_routes.bp)
    app.register_blueprint(sync_routes.bp)
    app.register_blueprint(rekordbox_routes.bp)

    @app.route('/status')
    def get_status():
        return {
            "status": "running",
            "version": "1.0",
            "env_vars": {
                "MASTER_TRACKS_DIRECTORY_SSD": app.config['MASTER_TRACKS_DIRECTORY_SSD'],
                "MASTER_PLAYLIST_ID": app.config['MASTER_PLAYLIST_ID']
            }
        }

    return app


# If running directly, create the app and run it
if __name__ == '__main__':
    app = create_app()
    app.run(debug=True, host="127.0.0.1", port=8765)
