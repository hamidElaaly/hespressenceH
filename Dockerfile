FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y software-properties-common gcc python3-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

    # Tools needed by vscode
    # trim down sources.list for faster operation \
RUN \
      export DEBIAN_FRONTEND="noninteractive" \
      && apt-get update \
      # basic tools for developer tooling \
      && apt-get install --no-install-recommends --yes jq wget curl unzip ca-certificates \
      && apt-get autoremove --yes \
      && apt-get clean --yes \
      && rm -rf /var/lib/{apt,dpkg,cache,log}/ \
      && echo "Done installing OS tools"
    
    # setup the Java runtime in the path
ENV JAVA_HOME=/opt/java 
ENV PATH=${JAVA_HOME}/bin:$PATH
    
    # install JDK, ref https://github.com/adoptium/api.adoptium.net/blob/main/docs/cookbook.adoc
RUN \
      echo "Installing JDK" \
      && if [ "$(uname -m)" = "x86_64" ]; then \
          ARCH="x64"; \
        elif [ "$(uname -m)" = "aarch64" ]; then \
          ARCH="aarch64"; \
        else \
          echo "Unsupported architecture"; exit 1; \
        fi \
      && echo "Arch: ${ARCH}" \
      && FEATURE_VERSION=$(curl -s https://api.adoptium.net/v3/info/available_releases | jq '.most_recent_feature_release') \
      && echo "JDK version [${FEATURE_VERSION}]" \
      && OS=linux && IMAGE_TYPE=jdk \
      && API_URL="https://api.adoptium.net/v3/binary/latest/${FEATURE_VERSION}/ga/${OS}/${ARCH}/${IMAGE_TYPE}/hotspot/normal/eclipse" \
      && echo "Downloading from ${API_URL}" \
      # Fetch the archive
      && FETCH_URL=$(curl -s -w %{redirect_url} "${API_URL}") \
      && echo "File fetch url: [${FETCH_URL}]" \
      # download the file
      && FILENAME=$(curl -OLs -w %{filename_effective} "${FETCH_URL}") \
      # Validate the checksum \
      && curl -Ls "${FETCH_URL}.sha256.txt" | sha256sum -c --status \
      && echo "Downloaded successfully as ${FILENAME}" \
      && mkdir -p ${JAVA_HOME} \
      && echo "extract jdk into jvm path (${JAVA_HOME})" \
      && echo "tar -xzvf ${FILENAME} -C ${JAVA_HOME} --strip-components=1" \
      && tar -xzvf ${FILENAME} -C ${JAVA_HOME} --strip-components=1 \
      && rm -f ${FILENAME} \
      && echo "Cleanup jdk distribution files not needed in image" \
      && rm -rf ${JAVA_HOME}/man ${JAVA_HOME}/man \
      && echo "Done"
    

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . .

# Download required Flink JARs
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-python/1.17.0/flink-python-1.17.0.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar && \
    wget -P /opt/flink/lib/ https://jdbc.postgresql.org/download/postgresql-42.7.5.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar && \
    wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.16.0/flink-json-1.16.0.jar

CMD ["python", "-m", "src.main"]
