# run the web interface "python3 web_run.py" etc...

from web import create_app
app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
