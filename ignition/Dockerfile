# Ignition 8.3 gateway, pre-provisioned with simulated line tags, a Kafka connection and an
# Event Stream that publishes tag changes to Kafka. Every module runs in Ignition's resettable
# 2-hour trial, so no license is needed for the demo.
# Image docs: https://hub.docker.com/r/inductiveautomation/ignition
ARG IGNITION_VERSION=8.3.9
FROM inductiveautomation/ignition:${IGNITION_VERSION}

# Gateway-scoped config goes in the "external" resource collection, which 8.3 provides for
# configuration supplied from outside the gateway. Writing to "core" before first boot
# faults commissioning ("Unable to create 'core' resource collection").
COPY --chown=ignition:ignition provision/external/ /usr/local/bin/ignition/data/config/resources/external/
COPY --chown=ignition:ignition provision/projects/ /usr/local/bin/ignition/data/projects/
COPY --chown=ignition:ignition kafka-connector.template.json line-to-kafka.template.json secret-provider.template.json /opt/provision/
COPY --chown=ignition:ignition --chmod=0755 render-and-start.sh /usr/local/bin/render-and-start.sh

ENV ACCEPT_IGNITION_EULA=Y \
    IGNITION_EDITION=standard \
    DISABLE_QUICKSTART=true

ENTRYPOINT ["render-and-start.sh"]
CMD ["-n", "ignition-kafka-demo"]
