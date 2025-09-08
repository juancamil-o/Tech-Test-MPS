from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_iam as iam,
)
from constructs import Construct
from pathlib import Path

class LambdaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, s3_bucket, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        lambda_role = iam.Role(
            self, "LambdaExecutionRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ]
        )

        s3_bucket.grant_put(lambda_role)

        self.extract_lambda = _lambda.Function(
            self, "ExtractContractsLambda",
            runtime=_lambda.Runtime.PYTHON_3_10,
            handler="lambda_function.handler", 
            code=_lambda.Code.from_asset("lambda_stack"),
            role=lambda_role,
            environment={
                "BUCKET_NAME": s3_bucket.bucket_name,
                "OUT_DIR": "/tmp/data.json"
            },
            timeout=Duration.minutes(5),
            memory_size=512,
        )
