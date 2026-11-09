"""In-memory S3 stand-in shared by tests: no network, no Docker."""

import io

from botocore.exceptions import ClientError


class FakeS3:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.metadata: dict[str, dict] = {}
        self.writes = 0
        self.deletes = 0

    def put_object(self, Bucket, Key, Body, Metadata=None, **kwargs):
        self.objects[Key] = Body
        self.metadata[Key] = Metadata or {}
        self.writes += 1

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def head_object(self, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"ContentLength": len(self.objects[Key]), "Metadata": self.metadata[Key]}

    def delete_object(self, Bucket, Key):
        self.objects.pop(Key, None)
        self.metadata.pop(Key, None)
        self.deletes += 1

    def list_objects_v2(self, Bucket, Prefix, ContinuationToken=None):
        keys = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}
