FROM python:3.11.4
WORKDIR /usr/src/app
COPY . .
RUN python -m pip install --upgrade pip
RUN pip install Flask 
RUN pip install -r requirements.txt
WORKDIR ./myapp
CMD gunicorn main:app --bind 0.0.0.0:5000
EXPOSE 5000

