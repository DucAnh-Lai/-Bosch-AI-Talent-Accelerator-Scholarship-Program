# set the base image. Since we're running
# a Python application a Python base image is used


FROM python:3.8
WORKDIR ./project/techtrends
LABEL maintainer="DucAnhLai"
ADD . /app

WORKDIR /app
RUN pip install -r requirements.txt
RUN python init_db.py
EXPOSE 3111
CMD [ "python", "app.py" ]
