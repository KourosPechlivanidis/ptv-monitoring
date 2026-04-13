FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Run dbt every 5 minutes; log failures but keep the loop alive
CMD ["sh", "-c", "while true; do dbt run --project-dir analytics --profiles-dir . && dbt test --project-dir analytics --profiles-dir . || echo '[dbt-runner] Run failed, retrying in 5 minutes'; sleep 300; done"]
