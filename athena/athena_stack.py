from aws_cdk import (
    Stack,
    aws_athena as athena,
    aws_iam as iam,
)
from constructs import Construct

class AthenaStack(Stack):
    def __init__(self, scope: Construct, id: str, *, data_bucket, athena_reader_arn, **kwargs):
        super().__init__(scope, id, **kwargs)
        
        data_bucket.add_to_resource_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{data_bucket.bucket_arn}/*"],
                principals=[iam.ArnPrincipal(athena_reader_arn)],
            )
        )

        self.workgroup = athena.CfnWorkGroup(
            self, "AthenaWorkgroup",
            name="DataPipelineWG",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{data_bucket.bucket_name}/results/"
                )
            )
        )