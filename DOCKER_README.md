# Docker Deployment Guide

This guide explains how to deploy the Zapier History Hacker application using Docker.

## Prerequisites

- Docker installed on your system
- Docker Compose (usually comes with Docker Desktop)

## Quick Start

### Option 1: Using Docker Compose (Recommended)

1. **Build and run the application:**

   ```bash
   docker-compose up --build
   ```

2. **Access the application:**
   Open your browser and navigate to `http://localhost:5000`

3. **Stop the application:**
   ```bash
   docker-compose down
   ```

### Option 2: Using Docker directly

1. **Build the Docker image:**

   ```bash
   docker build -t zapier-history-hacker .
   ```

2. **Run the container:**

   ```bash
   docker run -p 5000:5000 -v $(pwd)/uploads:/app/uploads zapier-history-hacker
   ```

3. **Access the application:**
   Open your browser and navigate to `http://localhost:5000`

## Docker Configuration Details

### Dockerfile Features

- **Base Image:** Python 3.11 slim for smaller image size
- **Security:** Runs as non-root user (`appuser`)
- **Production Ready:** Uses Gunicorn WSGI server
- **Health Checks:** Built-in health monitoring
- **Optimized:** Multi-stage build for better caching

### Environment Variables

- `FLASK_ENV=production`: Production environment
- `PORT=5000`: Application port
- `PYTHONUNBUFFERED=1`: Unbuffered Python output
- `PYTHONDONTWRITEBYTECODE=1`: Don't write .pyc files

### Volumes

- `./uploads:/app/uploads`: Persists uploaded files between container restarts

## Production Deployment

### Using Docker Compose

```bash
# Run in detached mode
docker-compose up -d --build

# View logs
docker-compose logs -f

# Stop the application
docker-compose down
```

### Using Docker Swarm

```bash
# Initialize swarm (if not already done)
docker swarm init

# Deploy the stack
docker stack deploy -c docker-compose.yml zapier-app

# List services
docker service ls

# Remove the stack
docker stack rm zapier-app
```

## Troubleshooting

### Common Issues

1. **Port already in use:**

   ```bash
   # Check what's using port 5000
   netstat -tulpn | grep :5000

   # Or change the port in docker-compose.yml
   ports:
     - "8080:5000"  # Use port 8080 instead
   ```

2. **Permission issues with uploads directory:**

   ```bash
   # Fix permissions
   sudo chown -R $USER:$USER uploads/
   chmod 755 uploads/
   ```

3. **Container won't start:**

   ```bash
   # Check container logs
   docker-compose logs

   # Rebuild without cache
   docker-compose build --no-cache
   ```

### Health Check

The application includes a health check that verifies the web server is responding:

```bash
# Check container health
docker ps
# Look for "healthy" status

# Manual health check
curl http://localhost:5000/
```

## Development with Docker

### Development Mode

For development, you can override the Dockerfile command:

```bash
# Run in development mode with Flask debug
docker run -p 5000:5000 -v $(pwd):/app zapier-history-hacker \
  python app.py
```

### Hot Reload

Mount your source code for hot reloading:

```yaml
# In docker-compose.yml
volumes:
  - .:/app
  - ./uploads:/app/uploads
```

## Security Considerations

- The application runs as a non-root user
- File uploads are restricted to JSON files only
- Maximum file size is limited to 16MB
- Temporary files are cleaned up automatically

## Performance Tuning

### Gunicorn Configuration

The application uses Gunicorn with the following settings:

- 2 worker processes
- 120-second timeout
- Bind to all interfaces (0.0.0.0)

You can customize these in the Dockerfile:

```dockerfile
CMD ["gunicorn", "--bind", "0.0.0.0:5000", "--workers", "4", "--timeout", "60", "app:app"]
```

### Resource Limits

Add resource limits in docker-compose.yml:

```yaml
services:
  zapier-history-hacker:
    # ... other config
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
        reservations:
          memory: 256M
          cpus: "0.25"
```
