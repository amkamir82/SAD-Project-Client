FROM python:3.10-slim-buster
WORKDIR /app
COPY ./ClientPython/requirements.txt /app
RUN pip install -r requirements.txt
COPY ./ClientPython .
EXPOSE 5000
ENV FLASK_APP=app
CMD ["python","main.py"]