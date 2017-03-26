FROM python:3
LABEL maintainer "John Doe john.doe@ccube.com"

COPY . /app

WORKDIR /app

RUN pip install -U pip && pip install -r requirements.txt

CMD ["python", "-m", "scheduler"]
