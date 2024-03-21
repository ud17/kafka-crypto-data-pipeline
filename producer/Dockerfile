FROM python:3.11.8-alpine

# Create app dir
RUN mkdir /producer

COPY ./ /producer

# Copy env variables
COPY .env /producer/.env

# Set the working dir
WORKDIR /producer

RUN pip3 install --no-cache-dir -r /producer/lib.txt

# Command to start the python script
CMD [ "python3", "main.py" ]