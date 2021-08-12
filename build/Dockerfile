# Stage 1: Use image builder to build the target binaries
# Copyright Contributors to the Open Cluster Management project

FROM golang:1.16 AS builder

ARG COMPONENT
WORKDIR /workspace/${COMPONENT}

COPY bin/leaf-hub-spec-sync bin/leaf-hub-spec-sync

# Stage 2: Copy the binaries from the image builder to the base image
FROM registry.access.redhat.com/ubi8/ubi-minimal:latest
ARG COMPONENT
ENV USER_UID=1001
ENV USER_NAME=${COMPONENT}

# install operator binary
COPY --from=builder /workspace/${COMPONENT}/bin/${COMPONENT} /usr/local/bin/manager

COPY build/scripts/user_setup /usr/local/scripts/user_setup
RUN  /usr/local/scripts/user_setup

RUN microdnf update && \
    microdnf clean all

# allow user to fully control the bin directory (for transport certificates) -- for now
RUN chown ${USER_UID}:0 /usr/local/bin
RUN chmod 0775 /usr/local/bin

USER ${USER_UID}
ENTRYPOINT ["/usr/local/bin/manager"]
