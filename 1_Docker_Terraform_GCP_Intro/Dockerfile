
#image you want to use as base image
FROM python:3.9.7

#Commands to execute
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 
#create our working directory in our container
WORKDIR /app
#copy our file to our working directory
##cmd source     destination
COPY ingest_data.py ingest_data.py
COPY output.csv output.csv
#make our entry point linux bash terminal
# ENTRYPOINT [ "bash" ]

#to run our app directly
ENTRYPOINT [ "python", "ingest_data.py" ]