FROM python:3.13-slim

WORKDIR /app
COPY services/recording_app/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt gunicorn

COPY shared /app/shared
COPY services/recording_app /app/services/recording_app

ENV PYTHONPATH=/app \
    RECORDING_STORAGE_ROOT=/data

VOLUME ["/data"]
EXPOSE 5001

CMD ["gunicorn", "--bind", "0.0.0.0:5001", "--workers", "2", "--threads", "4", "--timeout", "30", "services.recording_app.app:app"]
