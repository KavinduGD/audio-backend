FROM python:3.10.18-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

RUN export FLASK_DEBUG=1

EXPOSE 5000

CMD [ "flask", "run", "--host=0.0.0.0", "--port=5000" ]

