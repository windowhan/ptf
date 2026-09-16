# Product package injected into the shared runtime worker image
# (docs/first.md section 21.2 deployment model).
FROM runtime-worker:0.1.0

COPY dist/example_product-1.0.0-py3-none-any.whl /tmp/
RUN pip install /tmp/example_product-1.0.0-py3-none-any.whl

COPY runtime.yaml /app/runtime.yaml
