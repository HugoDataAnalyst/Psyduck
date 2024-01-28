FROM python:3.11
ENV TZ=Europe/Lisbon
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
WORKDIR /usr/src/app
COPY . .
RUN pip install -r requirements.txt

CMD [ "python", "celery_worker.py" ]
