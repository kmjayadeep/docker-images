FROM summerwind/actions-runner:v2.329.0-ubuntu-22.04

RUN sudo apt-get update && sudo apt-get install -y --no-install-recommends \
  cargo \
  pkg-config \
  libssl-dev \
  libudev-dev \
  g++ \
  gh \
  nodejs \
  openssh-client \
  wget

RUN wget -O - https://apt.releases.hashicorp.com/gpg | sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg
RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] https://apt.releases.hashicorp.com $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/hashicorp.list

RUN sudo apt-get update && sudo apt-get install -y --no-install-recommends \
  vault

# Fix vault operation not permitted error
RUN sudo setcap -r /usr/bin/vault
