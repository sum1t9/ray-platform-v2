FROM rayproject/ray:2.9.0-py311

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY ray_pipeline.py ./

ENV PYTHONUNBUFFERED=1

CMD ["python", "ray_pipeline.py"]