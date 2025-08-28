FROM flink:1.20.2-scala_2.12-java17

# Install Python and pip and JDK (needed for pemja compilation)
RUN apt-get update -y && \
    apt-get install -y python3 python3-pip python3-dev curl openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create a symlink for python command
RUN ln -s /usr/bin/python3 /usr/bin/python

# Upgrade pip
RUN python3 -m pip install --upgrade pip

# Install PyFlink (set JAVA_HOME for pemja compilation)
RUN export ARCH=$(dpkg --print-architecture) && \
    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-$ARCH" && \
    pip3 install apache-flink==1.20.2

# Download Kafka connector for Flink
RUN curl -L https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar

# Copy and install UDF requirements
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install -r /tmp/requirements.txt

# HuggingFace cache and offline-friendly defaults
ENV HF_HOME=/opt/hf_home
ENV TRANSFORMERS_CACHE=/opt/hf_home/hub
ENV HUGGINGFACE_HUB_CACHE=/opt/hf_home/hub
ENV HF_HUB_DISABLE_TELEMETRY=1
RUN mkdir -p /opt/hf_home/hub

# Preload model snapshots to cache for offline-friendly hot start
RUN python3 - <<'PY'
from huggingface_hub import snapshot_download

models = [
    "tabularisai/multilingual-sentiment-analysis",
    "JosefGoldstein/modernBERT-base-AIvsKaren-sentiment",
]

for repo_id in models:
    snapshot_download(repo_id=repo_id)
    print(f"Downloaded snapshot for {repo_id}")
PY

# Force offline usage at runtime so models are always loaded from the cached snapshot
ENV TRANSFORMERS_OFFLINE=1
ENV HF_HUB_OFFLINE=1

# Limit threading to avoid oversubscription by PyTorch/Tokenizers
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV TOKENIZERS_PARALLELISM=false

# Set Python path for Flink
ENV PYTHONPATH=/opt/flink/usrlib
ENV PYTHON_EXECUTABLE=python3

# Create usrlib directory
RUN mkdir -p /opt/flink/usrlib

# Copy entrypoint script
COPY entrypoint-wrapper.sh /opt/flink/entrypoint-wrapper.sh
RUN chmod +x /opt/flink/entrypoint-wrapper.sh

ENTRYPOINT ["/opt/flink/entrypoint-wrapper.sh"] 