FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt -t ${LAMBDA_TASK_ROOT}

COPY src/ ${LAMBDA_TASK_ROOT}

CMD ["handler.lambda_handler"]
