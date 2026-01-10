FROM public.ecr.aws/lambda/python:3.11

# Copy requirements
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy shared utilities
COPY ../shared ${LAMBDA_TASK_ROOT}/shared

# Copy handler
COPY src/handler.py ${LAMBDA_TASK_ROOT}

# Set handler
CMD ["handler.lambda_handler"]
