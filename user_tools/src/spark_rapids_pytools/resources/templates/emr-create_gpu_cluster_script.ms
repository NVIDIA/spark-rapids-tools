#!/bin/bash

export CLUSTER_NAME="{{ CLUSTER_NAME }}"

aws emr create-cluster \
    --name "$CLUSTER_NAME"  \
    --release-label {{ IMAGE }} \
    --log-uri s3://$LOG_BUCKET/logs \
    --applications Name=Hadoop Name=Spark Name=Livy Name=JupyterEnterpriseGateway  \
    --bootstrap-actions '[{"Path":"s3://BUCKET_NAME/aws-emr-bootstrap.sh","Name":"My Spark Rapids Bootstrap action"}]'  \
    --ec2-attributes '{"KeyName":"MY_KEY_NAME","InstanceProfile":"EMR_EC2_DefaultRole","AvailabilityZone":"{{ ZONE }}"}'  \
    --service-role EMR_DefaultRole \
    --instance-groups InstanceGroupType=MASTER,InstanceCount=1,InstanceType={{ MASTER_MACHINE }} \
                      InstanceGroupType=CORE,InstanceCount={{ WORKERS_COUNT }},InstanceType={{ WORKERS_MACHINE }} \
    --configurations file://aws-emr-configuration.json \
    --ebs-root-volume-size 100 \
