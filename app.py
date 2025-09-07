from aws_cdk import App, Environment
from s3.s3_stack import S3Stack
from lambda_stack.lambda_stack import LambdaStack
from glue.glue_stack import GlueStack
from lakeformation.lakeformation_stack import LakeFormationStack
from athena.athena_stack import AthenaStack
import os

app = App()
env = Environment(
    account=os.environ["CDK_DEFAULT_ACCOUNT"],
    region=os.environ["CDK_DEFAULT_REGION"]
)
athena_reader_arn = os.environ["ATHENA_READER_ARN"]
deployer_admin_arn = os.environ["DEPLOYER_ADMIN_ARN"]
# 1. S3
s3_stack = S3Stack(app, "S3Stack", env=env)

# 2. Lambda 
lambda_stack = LambdaStack(
    app, "LambdaStack",
    s3_bucket=s3_stack.data_bucket,
    env=env
)

# 3. Glue 
glue_stack = GlueStack(
    app, "GlueStack",
    data_bucket=s3_stack.data_bucket,
    database_name="data_pipeline_db",
    crawler_name="contracts-crawler",
    crawler_prefix="contracts/", 
    env=env
)

# 4. Lake Formation
lf_stack = LakeFormationStack(
    app, "LakeFormationStack",
    data_bucket=s3_stack.data_bucket,
    glue_db=glue_stack.glue_db,
    crawler_role=glue_stack.crawler_role,
    athena_reader_arn=athena_reader_arn,     
    deployer_admin_arn=deployer_admin_arn,
    target_table_name="ingestion_contracts",
    env=env
)



# 5. Athena
athena_stack = AthenaStack(
    app, "AthenaStack",
    data_bucket=s3_stack.data_bucket,
    env=env
)

# ===== Dependencies
lf_stack.add_dependency(s3_stack)
lf_stack.add_dependency(glue_stack)
lambda_stack.add_dependency(s3_stack)
glue_stack.add_dependency(s3_stack)
athena_stack.add_dependency(s3_stack)

app.synth()
