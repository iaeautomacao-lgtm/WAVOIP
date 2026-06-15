web: gunicorn -w 2 -b 0.0.0.0:$PORT --timeout 300 app:app
worker: celery -A tasks.celery worker --loglevel=info --concurrency=10 -Q celery
worker_import: celery -A tasks.celery worker --loglevel=info --concurrency=4 -Q imports
beat: celery -A tasks.celery beat --loglevel=info
