# To see all options, run `spark_rapids_user_tools dataproc bootstrap -- --help`

# The following cmd overrides the default Apache Spark configurations
# on the cluster (requires SSH)
spark_rapids_user_tools dataproc bootstrap \
    --cluster {{ CLUSTER_NAME }} \
    --verbose \
    --nodry_run

# The following cmd dumps the recommended configurations to the output
# without overriding the existing cluster configurations
spark_rapids_user_tools dataproc bootstrap \
    --cluster {{ CLUSTER_NAME }} \
    --verbose