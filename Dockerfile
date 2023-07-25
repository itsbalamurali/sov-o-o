FROM docker.stackable.tech/stackable/ubi8-rust-builder AS builder

FROM registry.access.redhat.com/ubi8/ubi-minimal AS operator

ARG VERSION
ARG RELEASE="1"

LABEL name="Sovrin Operator for Odoo" \
  maintainer="bala@sovrin.company" \
  vendor="Sovrin OÜ" \
  version="${VERSION}" \
  release="${RELEASE}" \
  summary="Deploy and manage Odoo instances." \
  description="Deploy and manage Odoo instances."

# Update image
RUN microdnf install -y yum \
  && yum -y update-minimal --security --sec-severity=Important --sec-severity=Critical \
  && yum clean all \
  && microdnf clean all

# Install kerberos client libraries
RUN microdnf install -y krb5-libs libkadm5 && microdnf clean all

COPY --from=builder /app/* /usr/local/bin/
COPY deploy/config-spec/properties.yaml /etc/stackable/odoo-operator/config-spec/properties.yaml

RUN groupadd -g 1000 sovrin && adduser -u 1000 -g sovrin -c 'Sovrin Operator' sovrin

USER sovrin:sovrin

ENTRYPOINT ["sovrin-odoo-operator"]
CMD ["run"]