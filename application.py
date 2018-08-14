import boto3
import logging
import json
import requests

from flask import Flask, request, Response
application = Flask(__name__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@application.route("/")
def index():
    return 'Ok'

@application.route("/replicate-file", methods=['POST'])
def replicate_file():

    response = None
    if request.json is None:
        # Expect application/json request
        response = Response("", status=415)
    else:
        try:
            # Get a service client for target region
            s3 = boto3.client('s3', request.json['TargetRegion'])
            # Get a service client for the source region
            source_client = boto3.client('s3', request.json['SourceRegion'])
            copy_source = {
                'Bucket': request.json['SourceBucket'],
                'Key': request.json['SourceKey']
            }
            s3.copy(copy_source, request.json['TargetBucket'], request.json['TargetKey'],
                    SourceClient=source_client)
            logger.info('file transfer complete')

            if 'Callback' in request.json:
                requests.post(request.json['Callback'], data=request.json)

            response = Response("", status=200)
        except Exception as ex:
            logging.exception('Error processing request: %s' % request.json)
            response = Response(ex.message, status=500)

    return response

if __name__ == "__main__":
    application.run()
