# To see all options, run `spark_rapids_user_tools dataproc emr -- --help`

spark_rapids_user_tools emr bootstrap \
    --cluster {{ CLUSTER_NAME }} \
    --verbose \
    --key_pair_path ${KEY_PATH} \
    --nodry_run