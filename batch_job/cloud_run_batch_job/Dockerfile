FROM python:3.9

COPY requirements.txt ./requirements.txt
RUN pip install -r requirements.txt --no-cache-dir

COPY ./ /app
WORKDIR /app

CMD ["python", "main.py"]