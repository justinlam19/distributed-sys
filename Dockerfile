# Local dev
FROM ubuntu

RUN apt-get update

# Install bash, git, and other packages
RUN apt-get install -y \
    bash \
    curl \
    gcc \
    git \
    musl-dev \
    sudo \
    vim \
    wget

RUN wget "https://go.dev/dl/go1.23.0.linux-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/').tar.gz" -O /tmp/go.tar.gz && rm -rf /usr/local/go && tar -C /usr/local -xzf /tmp/go.tar.gz
RUN curl -L "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')/kubectl" -o /usr/local/bin/kubectl && chmod +x /usr/local/bin/kubectl
RUN apt-get install -y docker.io docker-buildx
RUN apt-get install -y protobuf-compiler

ENV PATH="/usr/local/go/bin:${PATH}"
ENV CGO_ENABLED=1

# Install Go tools at specific version (Aug 2024)
RUN go install golang.org/x/tools/cmd/godoc@v0.24.0
RUN go install golang.org/x/tools/cmd/goimports@v0.24.0
RUN go install golang.org/x/tools/gopls@v0.16.1
RUN go install honnef.co/go/tools/cmd/staticcheck@v0.5.1

# Install gRPC and protobuf plugins for Go (Aug 2024)
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1 \
    && go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2

ARG USER=nonroot

RUN useradd -m --groups sudo --shell /bin/bash ${USER} \
    && echo "${USER} ALL=(ALL) NOPASSWD:ALL" >/etc/sudoers.d/${USER} \
    && chmod 0440 /etc/sudoers.d/${USER} \
    && chown -R ${USER}:${USER} /home/${USER}

USER ${USER}
WORKDIR /home/${USER}

ENV HOME=/home/$USER
RUN echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.profile \
    && echo 'export CGO_ENABLED=1' >> ~/.profile

CMD ["/bin/bash"]
