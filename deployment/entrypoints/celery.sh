#!/bin/bash

CELERY_YML=${DAGSTER_HOME}/celery.yml
cat <<EOF > $CELERY_YML
---
execution:
  config:
    broker: pyamqp://$DAGSTER_RABBITMQ_USERNAME:$DAGSTER_RABBITMQ_PASSWORD@$DAGSTER_RABBITMQ_HOST:5672/$DAGSTER_RABBITMQ_VHOST  # Use the broker container hostname
    backend: rpc://  # Or use a different backend like Redis

EOF

echo "Created celery config file."
dagster-celery worker start --app dagster_celery.app --config-yaml $CELERY_YML
