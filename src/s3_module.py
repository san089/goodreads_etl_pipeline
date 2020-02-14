import boto3
import configparser
import logging

logger = logging.getLogger(__name__)

config = configparser.ConfigParser()
config.read_file(open('config.cfg'))

class GoodReadsS3Module:

    def __init__(self):
        self._s3 = boto3\
                    .resource(service_name = 's3', region_name = 'us-east-1', aws_access_key_id=config.get('AWS', 'Key'), aws_secret_access_key=config.get('AWS', 'SECRET'))
        self._files = []
        self._landing_zone = config.get('BUCKET','LANDING_ZONE')
        self._working_zone = config.get('BUCKET','WORKING_ZONE')
        self._processed_zone = config.get('BUCKET','PROCESSED_ZONE')

    def s3_move_data(self, source_bucket = None, target_bucket= None):

        if source_bucket is None:
            source_bucket = self._landing_zone
        if target_bucket is None:
            target_bucket = self._working_zone

        # cleanup target bucket
        self.clean_bucket(target_bucket)

        # Move files to working zone
        for key in self.check_files_exists(source_bucket):
            if key in config.get('FILES','NAME').split(","):
                self._s3.meta.client.copy({'Bucket': source_bucket, 'Key': key}, target_bucket, key)

        # cleanup source bucket
        self.clean_bucket(source_bucket)

    def check_files_exists(self, bucket_name):
        return [bucket_object.key for bucket_object in self._s3.Bucket(bucket_name).objects.all()]

    def clean_bucket(self, bucket_name):
        self._s3.Bucket(bucket_name).objects.all().delete()