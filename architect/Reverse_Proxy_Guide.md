# System Prompt & Instruction Guide: Implementing a Reverse Proxy Pattern for Flask

**Target Audience:** AI Coding Assistant / Developer Agent
**Objective:** Refactor an existing Flask-based Language Annotation application to use a Reverse Proxy architecture.

## 1. Architectural Overview
Do not expose Flask's built-in development server (Werkzeug) or standalone WSGI servers directly to the public internet or end users. Instead, route all incoming traffic through a **Reverse Proxy (Nginx)**, which will act as the API Gateway and route traffic to the appropriate internal Flask microservice (Dashboard, Recorder, Annotator) running via **Gunicorn**.

### Target Folder Structure
Ensure the project is organized to support separate services and proxy configurations:
```text
project_root/
│
├── docker-compose.yml       # Orchestrates the proxy and services
│
├── nginx/
│   └── nginx.conf           # Reverse Proxy configuration
│
├── dashboard_service/
│   ├── app.py               # Flask app (Port 5000 internally)
│   ├── requirements.txt
│   └── Dockerfile
│
├── recorder_service/
│   ├── app.py               # Flask app (Port 5001 internally)
│   ├── requirements.txt
│   └── Dockerfile
```

---

## 2. Step-by-Step Implementation Instructions for the AI Agent

### Step 1: Wrap Flask Apps with `ProxyFix`
**Instruction:** When a Flask app sits behind a reverse proxy, the `Request.remote_addr` and `Request.scheme` will reflect the proxy's internal IP and HTTP scheme, not the actual client's. You must wrap the Flask app with Werkzeug's `ProxyFix` in every service.

**Action:** Update `app.py` in all services.
```python
# app.py (Example for Dashboard Service)
from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix

app = Flask(__name__)

# Trust the X-Forwarded-* headers sent by Nginx
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)

@app.route('/api/v1/dashboard/')
def index():
    return {"message": "Dashboard Service Active"}

if __name__ == '__main__':
    # Do NOT run this in production. Use Gunicorn.
    app.run(host='0.0.0.0', port=5000)
```

### Step 2: Configure the Reverse Proxy (Nginx)
**Instruction:** Create an Nginx configuration file that listens on port 80 and routes traffic based on the URL path. Use trailing slashes carefully to avoid route stripping issues.

**Action:** Generate `nginx/nginx.conf`.
```nginx
events {
    worker_connections 1024;
}

http {
    server {
        listen 80;
        server_name localhost;

        # Route to Dashboard Service
        location /api/v1/dashboard/ {
            proxy_pass http://dashboard_service:5000/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }

        # Route to Recorder Service
        location /api/v1/recorder/ {
            proxy_pass http://recorder_service:5001/;
            proxy_set_header Host $host;
            proxy_set_header X-Real-IP $remote_addr;
            proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
            proxy_set_header X-Forwarded-Proto $scheme;
        }
    }
}
```

### Step 3: Containerize with Docker Compose
**Instruction:** To ensure isolated environments and internal DNS resolution (so Nginx can route to `http://dashboard_service:5000`), generate a `docker-compose.yml` file. Define Gunicorn as the startup command for the Flask services.

**Action:** Generate `docker-compose.yml`.
```yaml
version: '3.8'

services:
  reverse_proxy:
    image: nginx:latest
    ports:
      - "80:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - dashboard_service
      - recorder_service

  dashboard_service:
    build: ./dashboard_service
    # Bind to 0.0.0.0 internally, do NOT expose ports to the host network
    command: gunicorn -w 4 -b 0.0.0.0:5000 app:app
    expose:
      - "5000"

  recorder_service:
    build: ./recorder_service
    command: gunicorn -w 4 -b 0.0.0.0:5001 app:app
    expose:
      - "5001"
```

## 3. Verification & Testing
**Instruction:** After writing the files, the AI Agent should instruct the user to run the following commands to verify the setup:

1. Start the cluster: `docker-compose up --build -d`
2. Test Dashboard: `curl http://localhost/api/v1/dashboard/`
3. Test Recorder: `curl http://localhost/api/v1/recorder/`

If successful, the reverse proxy is successfully hiding the internal ports (`5000`, `5001`) from the user while managing the microservice routing.
