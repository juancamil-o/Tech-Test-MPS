from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
)
from constructs import Construct


class S3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)


        self.data_bucket = s3.Bucket(
            self, "DataLakeBucket",
            versioned=True,
            removal_policy=RemovalPolicy.DESTROY,  
            auto_delete_objects=True               
        )

        self.add_output("DataLakeBucketName", self.data_bucket.bucket_name)

    def add_output(self, name: str, value: str):
        """Helper para exportar valores del stack"""
        from aws_cdk import CfnOutput
        CfnOutput(self, name, value=value)
