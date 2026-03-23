FROM python:3.11

WORKDIR /app

ENV DATABASE_URL=postgresql://admin:sumanth123@172.17.0.1:5432/placement

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "9723"]