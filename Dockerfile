FROM quay.io/astronomer/astro-runtime:12.6.0

# Instalar dependências do sistema (se precisar)
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Volta para usuário airflow
USER astro

# Copia requirements (opcional, mas recomendado)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt