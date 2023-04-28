FROM python:3.8

RUN apt-get update && apt-get install -y default-jre

RUN mkdir -p /app
WORKDIR /app

COPY . /app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]
