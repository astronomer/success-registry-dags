FROM quay.io/astronomer/ap-airflow:2.2.0-buster-onbuild
ENV AIRFLOW__SECRETS__BACKEND="airflow.providers.amazon.aws.secrets.systems_manager.SystemsManagerParameterStoreBackend"
ENV AIRFLOW__SECRETS__BACKEND_KWARGS='{"connections_prefix": "/TEMP/CONN", "variables_prefix": "/TEMP/VAR"}'