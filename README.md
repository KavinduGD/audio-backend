# Audio Backend Service

This repository contains the backend service for the Audio Admin Panel, built with Python and Flask. It handles audio file processing, data management via DynamoDB/S3, and integrates with AWS AI services.

## Architecture

The system architecture works as follows:

### High Level Overview
![High Level Architecture](assets/architecture_high_level.png)

### Detailed Architecture
![Detailed Architecture](assets/architecture_detailed.png)

## Tech Stack

- **Language:** Python 3.10
- **Framework:** Flask
- **Audio Processing:** `librosa`, `soundfile`, `pydub`
- **Data Handling:** `pandas`, `numpy`
- **AWS Integration:** `boto3`, `sagemaker`, `s3transfer`
- **Database:** DynamoDB / S3 (via AWS SDK)

## Project Structure

```
audio-backend/
├── app/                 # Application source code
├── tests/               # Unit and integration tests
├── .github/workflows/   # CI/CD configurations
├── requirements.txt     # Python dependencies
├── run.py               # Application entry point
├── Dockerfile           # Container definition
└── compose.yaml         # Docker Compose configuration
```

## Local Development

### Prerequisites

- Python 3.10+
- pip
- Virtual environment (recommended)

### Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd audio-backend
    ```

2.  **Create and activate a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows: .venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Run the application:**
    ```bash
    python run.py
    ```

## Deployment

The project uses **GitHub Actions** for Continuous Integration and Continuous Deployment (CI/CD) to **AWS ECS (Fargate)**.

### Workflow

1.  **Trigger:** A pull request is merged into the `main` branch.
2.  **Build & Test:**
    - Sets up Python environment.
    - Installs dependencies.
    - Runs tests using `pytest`.
3.  **Build & Push Image:**
    - Logs into Amazon ECR.
    - Builds the Docker image.
    - Pushes the image to the ECR repository: `audio-backend`.
4.  **Deploy:**
    - Updates the AWS ECS service `audio-backend-service` in cluster `audio-cluster`.
    - Forces a new deployment to pick up the latest image.

## Environment Variables

Ensure the following secrets are configured in your GitHub repository for deployment:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
