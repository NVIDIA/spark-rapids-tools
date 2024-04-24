#!/bin/bash

export CLUSTER_NAME="{{ CLUSTER_NAME }}"

gcloud dataproc clusters create $CLUSTER_NAME \
    --image-version={{ IMAGE }} \
    --region {{ REGION }} \
    --zone {{ ZONE }} \
    --master-machine-type {{ MASTER_MACHINE }} \
    --num-workers {{ WORKERS_COUNT }} \
    --worker-machine-type {{ WORKERS_MACHINE }} \
    --num-worker-local-ssds {{ LOCAL_SSD }} \
    --enable-component-gateway \
    --subnet=default \
    --initialization-actions=gs://goog-dataproc-initialization-actions-{{ REGION }}/spark-rapids/spark-rapids.sh \
    --worker-accelerator type={{ GPU_DEVICE }},count={{ GPU_PER_WORKER }} \
    --properties 'spark:spark.driver.memory=50g'