import boto3
import logging
import requests

from botocore.exceptions import ClientError
from flask import Flask, request, Response

application = Flask(__name__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

@application.route("/")
def index():
    return 'Ok'

@application.route("/replicate-file", methods=['POST'])
def replicate_file():

    if request.json is None:
        # Expect application/json request
        response = Response("", status=415)
    else:
        try:
            SourceKey = request.json['SourceKey']

            SourceRegion = request.json['SourceRegion']
            SourceBucket = request.json['SourceBucket']
            SourceFolder = request.json['SourceFolder']

            TargetRegion = request.json['TargetRegion']
            TargetBucket = request.json['TargetBucket']
            TargetFolder = request.json['TargetFolder']

            # Get the S3 clients
            source_client = boto3.client('s3', SourceRegion)
            target_client = boto3.client('s3', TargetRegion)

            # Begin defining the keys needed for list_objects_v2, we do this way on purpose
            # list_objects_v2() returns up to 1000 keys at a time
            kwargs = {'Bucket': SourceBucket}
            kwargs['Prefix'] = SourceKey

            # Begin a loop so we transfer all files no matter how many there are
            while True:
                # Make the call to list_obects_v2
                objects = source_client.list_objects_v2(**kwargs)

                # Loop through our matching files
                for file in objects['Contents']:
                    Key = file['Key']
                    TargetKey = Key.replace(SourceFolder, TargetFolder)

                    # Copy the source to the target
                    copy_source = {
                        'Bucket': SourceBucket,
                        'Key': Key
                    }
                    target_client.copy(copy_source, TargetBucket, TargetKey, SourceClient=source_client)

                # The S3 API is paginated, returning up to 1000 keys at a time.
                # Pass the continuation token into the next response, until we
                # reach the final page (when this field is missing).
                # This ensures we copy all files related to this key
                try:
                    kwargs['ContinuationToken'] = objects['NextContinuationToken']
                except KeyError:
                    break


            logger.info('File Transfer Complete')

            if 'Callback' in request.json:
                requests.post(request.json['Callback'], data=request.json)

            response = Response("", status=200)

        except ClientError as ex:
            if ex.response['Error']['Code'] == '404':
                logger.error('Error processing request: %s' % request.json)
                if 'Callback' in request.json:
                    request.json['NoSourceFile'] = 1
                    requests.post(request.json['Callback'], data=request.json)
                response = Response("", status=200)

            else:
                logger.error('Error processing request: %s' % request.json)
                logger.error('Error: %s' % str(ex))
                response = Response("", status=500)

    return response

if __name__ == "__main__":
    application.run()
