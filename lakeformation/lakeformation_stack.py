from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_lakeformation as lf,
    CfnDeletionPolicy,
)
from constructs import Construct


class LakeFormationStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        *,
        data_bucket,               
        glue_db,                   
        crawler_role,              
        athena_reader_arn: str,    
        deployer_admin_arn: str,   
        target_table_name: str,            
        excluded_columns: list[str] = None, 
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        excluded_columns = excluded_columns or []  

        # (0) Lake Formation admins (root + deployer + cfn-exec)
        lf_admins = lf.CfnDataLakeSettings(
            self, "LFAdmins",
            admins=[
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=f"arn:aws:iam::{self.account}:root"
                ),
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=deployer_admin_arn
                ),
                lf.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=(
                        f"arn:aws:iam::{self.account}:role/"
                        f"cdk-hnb659fds-cfn-exec-role-{self.account}-{self.region}"
                    )
                ),
            ]
        )

        # (1) Registry data location
        lf_location = lf.CfnResource(
            self, "LFDataLocation",
            resource_arn=data_bucket.bucket_arn,
            use_service_linked_role=True
        )
        lf_location.add_dependency(lf_admins)
        lf_location.cfn_options.deletion_policy = CfnDeletionPolicy.RETAIN
        lf_location.cfn_options.update_replace_policy = CfnDeletionPolicy.RETAIN

        db_name = glue_db.ref 

        # (2) DATA_LOCATION_ACCESS for Crawler
        crawler_loc_perm = lf.CfnPermissions(
            self, "CrawlerDataLocationPerm",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    catalog_id=self.account,
                    s3_resource=data_bucket.bucket_arn
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"],
        )
        crawler_loc_perm.add_dependency(lf_admins)
        crawler_loc_perm.add_dependency(lf_location)

        # (3) Crawler: grants over DB (create/alter tables)
        crawler_db_perm = lf.CfnPermissions(
            self, "CrawlerDatabasePerm",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=crawler_role.role_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=self.account,
                    name=db_name
                )
            ),
            permissions=["CREATE_TABLE", "ALTER"],
        )
        crawler_db_perm.add_dependency(lf_admins)
        crawler_db_perm.add_dependency(lf_location)

        #  (4) Athena

        # 4a) DATA_LOCATION_ACCESS a
        reader_loc_perm = lf.CfnPermissions(
            self, "AthenaReaderDataLocation",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_reader_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                data_location_resource=lf.CfnPermissions.DataLocationResourceProperty(
                    catalog_id=self.account,
                    s3_resource=data_bucket.bucket_arn
                )
            ),
            permissions=["DATA_LOCATION_ACCESS"],
        )
        reader_loc_perm.add_dependency(lf_admins)
        reader_loc_perm.add_dependency(lf_location)

        # 4b) DB level: only DESCRIBE
        reader_db_describe = lf.CfnPermissions(
            self, "AthenaReaderDBDescribe",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_reader_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                database_resource=lf.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=self.account,
                    name=db_name
                )
            ),
            permissions=["DESCRIBE"],
        )
        reader_db_describe.add_dependency(lf_admins)

        #Remove docstring to add table and column level permissions
        #and re-deploy LakeFormation Stack once glue crawler has been activated once
        '''
        # 4c) table level: DESCRIBE (schema)
        reader_table_describe = lf.CfnPermissions(
            self, "AthenaReaderTableDescribe",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_reader_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_resource=lf.CfnPermissions.TableResourceProperty(
                    catalog_id=self.account,
                    database_name=db_name,
                    name=target_table_name
                )
            ),
            permissions=["DESCRIBE"]
        )
        reader_table_describe.add_dependency(reader_db_describe)

        # 4d) excluded columns
        reader_select_allowed_cols = lf.CfnPermissions(
            self, "AthenaReaderSelectAllowedColumns",
            data_lake_principal=lf.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=athena_reader_arn
            ),
            resource=lf.CfnPermissions.ResourceProperty(
                table_with_columns_resource=lf.CfnPermissions.TableWithColumnsResourceProperty(
                    catalog_id=self.account,
                    database_name=db_name,
                    name=target_table_name,
                    column_wildcard=lf.CfnPermissions.ColumnWildcardProperty(
                        excluded_column_names=excluded_columns  # <-- tú rellenas esta lista
                    )
                )
            ),
            permissions=["SELECT"]
        )
        reader_select_allowed_cols.add_dependency(reader_table_describe)
        '''