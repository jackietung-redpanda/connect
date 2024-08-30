import json
import unittest
import boto3
from moto import mock_aws
from goreleaser_binaries_to_s3 import S3BucketClient, PluginConfig
import os
from click.testing import CliRunner
from goreleaser_binaries_to_s3 import cli

TEST_BUCKET = "my-bucket"
TEST_REGION = "my-region"
TEST_PLUGIN = PluginConfig(
    plugin_name="cow",
    binary_name="redpanda-cow"
)


def create_bucket_and_return_clients():
    """Create TEST_BUCKET bucket and return a S3BucketClient for it."""
    client = boto3.client("s3", region_name=TEST_REGION)
    client.create_bucket(Bucket=TEST_BUCKET, CreateBucketConfiguration={"LocationConstraint": TEST_REGION})

    # S3BucketClient, boto3 S3 client
    return S3BucketClient(TEST_BUCKET, TEST_REGION), client


class TestS3BucketClient(unittest.TestCase):
    @mock_aws
    def test_list_dir_recursive(self):
        bucket_client, _ = create_bucket_and_return_clients()
        keys_added = set()
        for i in range(2048):
            key = f"root/{i}/{i}"
            keys_added.add(key)
            bucket_client.upload_blob_with_tags(object_path=key, data=b"")
        found_keys = bucket_client.list_dir_recursive('root')
        assert set(found_keys) == keys_added


RESIDENT_DIR_PATH = os.path.dirname(os.path.realpath(__file__))
# "test_data" here would map to root of the real go project (like root of connect repo)
TEST_DATA_DIR_PATH = f"{RESIDENT_DIR_PATH}/test_data"


class TestUploadArchives(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        import goreleaser_binaries_to_s3
        assert TEST_PLUGIN.plugin_name not in goreleaser_binaries_to_s3.PLUGIN_CONFIGS
        goreleaser_binaries_to_s3.PLUGIN_CONFIGS[TEST_PLUGIN.plugin_name] = TEST_PLUGIN

    @mock_aws
    def test_end_to_end_upload(self):
        """Run upload-archives, then upload-manifest
        verify all archives and correct manifest uploaded"""
        bucket_client, s3_client = create_bucket_and_return_clients()

        runner = CliRunner()

        ARTIFACTS_FILE = f"{TEST_DATA_DIR_PATH}/dist/artifacts.json"

        def _run_and_validate_upload_archives(metadata_file: str, expected_keys: set[str]):
            # make bucket_client early, ensures bucket is created before we run the command
            existing_cwd = os.getcwd()

            try:
                os.chdir(TEST_DATA_DIR_PATH)
                # TODO rename to --artifacts-file
                _result = runner.invoke(cli, ['upload-archives',
                                              f'--artifacts-file={ARTIFACTS_FILE}',
                                              f'--metadata-file={metadata_file}',
                                              f'--region={TEST_REGION}',
                                              f'--bucket={TEST_BUCKET}',
                                              f'--plugin={TEST_PLUGIN.plugin_name}',
                                              f'--goos=linux,darwin,windows',
                                              f'--goarch=amd64,arm64,turing'],
                                        # TODO check if regular cli execution also transparent re: exceptions (we want that)
                                        catch_exceptions=False)
                assert _result.exit_code == 0
            finally:
                os.chdir(existing_cwd)
            found_keys = set(bucket_client.list_dir_recursive())

            assert found_keys == expected_keys

        # upload-archives (first run, for version v4.34.0)
        _run_and_validate_upload_archives(metadata_file=f"{TEST_DATA_DIR_PATH}/dist/metadata_v4_34_0.json",
                                          expected_keys={'cow/archives/4.34.0/redpanda-cow-darwin-arm64.tar.gz',
                                                         'cow/archives/4.34.0/redpanda-cow-linux-amd64.tar.gz'})

        # upload-archives (second run, for version v4.35.0)
        _run_and_validate_upload_archives(metadata_file=f"{TEST_DATA_DIR_PATH}/dist/metadata_v4_35_0.json",
                                          expected_keys={'cow/archives/4.34.0/redpanda-cow-darwin-arm64.tar.gz',
                                                         'cow/archives/4.34.0/redpanda-cow-linux-amd64.tar.gz',
                                                         'cow/archives/4.35.0/redpanda-cow-darwin-arm64.tar.gz',
                                                         'cow/archives/4.35.0/redpanda-cow-linux-amd64.tar.gz'})

        # upload-manifests (verify both versions of archives show up in manifest.json)
        result = runner.invoke(cli, ['upload-manifest',
                                     f'--region={TEST_REGION}',
                                     f'--bucket={TEST_BUCKET}',
                                     f'--plugin={TEST_PLUGIN.plugin_name}',
                                     f'--repo-hostname=cow.farm.com'],
                               catch_exceptions=False)
        assert result.exit_code == 0
        response = s3_client.get_object(Bucket=TEST_BUCKET, Key="cow/manifest.json")
        found_manifest = json.load(response['Body'])
        expected_manifest = {
            "archives": [
                {
                    "artifacts": {
                        "darwin-arm64": {
                            "path": "https://cow.farm.com/cow/archives/4.34.0/redpanda-cow-darwin-arm64.tar.gz",
                            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        },
                        "linux-amd64": {
                            "path": "https://cow.farm.com/cow/archives/4.34.0/redpanda-cow-linux-amd64.tar.gz",
                            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        }
                    },
                    "version": "4.34.0"
                },
                {
                    "artifacts": {
                        "darwin-arm64": {
                            "path": "https://cow.farm.com/cow/archives/4.35.0/redpanda-cow-darwin-arm64.tar.gz",
                            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        },
                        "linux-amd64": {
                            "path": "https://cow.farm.com/cow/archives/4.35.0/redpanda-cow-linux-amd64.tar.gz",
                            "sha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
                        }
                    },
                    "is_latest": True,
                    "version": "4.35.0"
                }
            ],
            "created_at": 1700000000
        }

        # align created_at - that is always different
        found_manifest['created_at'] = 1700000000
        assert expected_manifest == found_manifest


if __name__ == "__main__":
    unittest.main()
