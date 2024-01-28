FROM python:3.11
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt

CMD [ "python", "celery_worker.py" ]
