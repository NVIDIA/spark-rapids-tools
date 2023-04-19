# To see all options, run `spark_rapids_user_tools emr bootstrap -- --help`

# The following cmd overrides the default Apache Spark configurations
# on the cluster (requires SSH)
spark_rapids_user_tools emr bootstrap \
    --cluster {{ CLUSTER_NAME }} \
    --verbose \
    --key_pair_path ${KEY_PATH} \
    --nodry_run

# The following cmd dumps the recommended configurations to the output
# without overriding the existing cluster configurations
spark_rapids_user_tools emr bootstrap \
    --cluster {{ CLUSTER_NAME }} \
    --verbose