import os
import sys
from app import app

if __name__ == '__main__':
    # Determine if running in production mode
    PROD_MODE = os.environ.get('PROD_MODE', '0') == '1'
    PORT = int(os.environ.get('PORT', 5000))

    if PROD_MODE:
        # In production, listen only on localhost
        app.run(host='127.0.0.1', port=PORT, debug=False)
    else:
        # In development, allow external connections
        app.run(host='0.0.0.0', port=PORT, debug=True)
