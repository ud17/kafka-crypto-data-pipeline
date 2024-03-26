FROM python:3.11.8-alpine

# Create app dir
RUN mkdir /consumer

COPY ./ /consumer

# Copy env variables
COPY .env /consumer/.env

# Set the working dir
WORKDIR /consumer

RUN pip3 install --no-cache-dir -r /consumer/lib.txt

# Command to start the python script
CMD [ "python3", "consumer-dynamodb.py" ]