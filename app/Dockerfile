FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
COPY setup.py .
COPY pyproject.toml .
RUN pip install --no-cache-dir -r requirements.txt
COPY clearcare_compliance ./clearcare_compliance
COPY app ./app
COPY rules ./rules
RUN pip install -e .
EXPOSE 8000
CMD ["uvicorn", "app.api:app", "--host", "0.0.0.0", "--port", "8000"]
