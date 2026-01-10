FROM public.ecr.aws/lambda/python:3.11

COPY lambdas/enrich_to_s3/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t ${LAMBDA_TASK_ROOT}

COPY lambdas/enrich_to_s3/src/ ${LAMBDA_TASK_ROOT}

CMD ["handler.lambda_handler"]
