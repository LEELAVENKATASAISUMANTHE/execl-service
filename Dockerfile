FROM python:3.11

WORKDIR /service

ENV DATABASE_URL=postgresql://admin:sumanth123@172.17.0.1:5432/placement

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

EXPOSE 9723

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9723"]
```

---

## Final Structure On Server

Make sure the server looks exactly like this:
```
~/python-service/
├── main.py
├── db.py
├── config.py
├── schema.py
├── routes/
│   ├── __init__.py        ← must exist, even if empty
│   └── schema_routes.py
├── Dockerfile
└── requirements.txt