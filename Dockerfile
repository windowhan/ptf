# Runtime deploy image — installs the distributed-runtime wheel with the
# GCP/Postgres extras plus any product wheels present in dist/ at build
# time. One image serves all roles:
#   python -m distributed_runtime {migrate|worker|control}
FROM python:3.12-slim
WORKDIR /app
COPY dist/*.whl /tmp/wheels/
RUN pip install --no-cache-dir \
      "distributed-runtime[gcp,postgres] @ file:///tmp/wheels/distributed_runtime-0.1.0-py3-none-any.whl" \
      $(find /tmp/wheels -name '*.whl' ! -name 'distributed_runtime-*') \
 && rm -rf /tmp/wheels
ENTRYPOINT ["python", "-m", "distributed_runtime"]
