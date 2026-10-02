FROM python:3.11-slim
WORKDIR /service
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
COPY scripts scripts
COPY models models
COPY examples examples
COPY .streamlit .streamlit
RUN useradd --uid 10001 --create-home appuser
USER appuser
CMD ["python", "-m", "app.scoring_worker"]
