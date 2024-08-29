import unittest
import boto3
from moto import mock_aws
from goreleaser_binaries_to_s3 import S3BucketClient


class TestS3BucketClient(unittest.TestCase):
    @mock_aws
    def test_list_dir_recursive(self):
        bucket = "my-bucket"
        region = "my-region"
        client = boto3.client("s3", region_name=region)
        client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})

        bucket_client = S3BucketClient(bucket, region)
        keys_added = set()
        for i in range(2048):
            key = f"root/{i}/{i}"
            keys_added.add(key)
            bucket_client.upload_blob_with_tags(object_path=key, data=b"")
        found_keys = bucket_client.list_dir_recursive('root')
        self.assertEqual(set(found_keys), keys_added)


class TestUploadArchives(unittest.TestCase):

    """
      • building binaries
• building                                       binary=target/dist/connect_freebsd_arm64/redpanda-connect
• building                                       binary=target/dist/connect_windows_arm64/redpanda-connect.exe
• building                                       binary=target/dist/connect_darwin_arm64/redpanda-connect
• building                                       binary=target/dist/connect_linux_amd64_v1/redpanda-connect
• building                                       binary=target/dist/connect_freebsd_amd64_v1/redpanda-connect
• building                                       binary=target/dist/connect_linux_arm64/redpanda-connect
• building                                       binary=target/dist/connect_windows_amd64_v1/redpanda-connect.exe
• building                                       binary=target/dist/connect_darwin_amd64_v1/redpanda-connect
    """


if __name__ == "__main__":
    unittest.main()
