FROM python:3.14-slim

WORKDIR /usr/src/app
COPY . .
RUN python3 -m pip install --no-cache-dir -r requirements.txt
