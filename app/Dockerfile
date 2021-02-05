FROM python:latest

RUN mkdir /app
WORKDIR /app

COPY ./requirements.txt /app
RUN pip install -r requirements.txt

COPY app.py /app
CMD ["python", "-u", "app.py"]
