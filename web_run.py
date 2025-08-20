# run the web interface "python3 web_run.py" etc...

#!/usr/bin/env python3
import os
from web import create_app   # pulls from web/__init__.py

app = create_app()

if __name__ == "__main__":
    host = os.environ.get("FLASK_HOST", "0.0.0.0")
    port = int(os.environ.get("FLASK_PORT", "8080"))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    app.run(host=host, port=port, debug=debug)

