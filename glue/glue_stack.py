from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_glue as glue,
)
from constructs import Construct
import json

class GlueStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        data_bucket,                
        database_name: str = "data_pipeline_db",
        crawler_name: str = "bronze-crawler",
        crawler_prefix: str = "contracts/",
        crawler_schedule: str | None = None, 
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Glue Database
        self.glue_db = glue.CfnDatabase(
            self,
            "GlueDatabase",
            catalog_id=self.account,
            database_input={"name": database_name},
        )

        # IAM Role 
        self.crawler_role = iam.Role(
            self, "GlueCrawlerRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            # Política gestionada base para Glue (logs, etc.)
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole")
            ],
        )

        # Minimum Privilege
        self.crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[data_bucket.bucket_arn],
                conditions={"StringLike": {"s3:prefix": [f"{crawler_prefix}*",]}}
            )
        )
        self.crawler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{data_bucket.bucket_arn}/*"]
            )
        )


        # Glue Crawler pointing data prefix
        s3_target_path = f"s3://{data_bucket.bucket_name}/{crawler_prefix}"

        crawler_configuration = {
            "Version": 1.0,
            "CrawlerOutput": {
                "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"}
            },
            "Grouping": {"TableGroupingPolicy": "CombineCompatibleSchemas"},
        }

        self.crawler = glue.CfnCrawler(
            self,
            "DataCrawler",
            name=crawler_name,
            role=self.crawler_role.role_arn,
            database_name=self.glue_db.ref,
            targets={"s3Targets": [{"path": s3_target_path}]},
            schema_change_policy={
                "deleteBehavior": "LOG",            
                "updateBehavior": "UPDATE_IN_DATABASE"
            },
            configuration=json_dumps(crawler_configuration),
            schedule={"scheduleExpression": crawler_schedule} if crawler_schedule else None,
            recrawl_policy={"recrawlBehavior": "CRAWL_EVERYTHING"},  
        )

        # Crawler creation after DB
        self.crawler.add_dependency(self.glue_db)

def json_dumps(obj) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)
