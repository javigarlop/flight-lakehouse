# Grafana with the Trino data source plugin baked in, so it starts without downloading
# anything. The plugin lives outside /var/lib/grafana: that path is a volume, and a plugin
# copied there would stay at its old version after an image upgrade.
FROM grafana/grafana:13.3.1

ARG TRINO_PLUGIN_VERSION=1.4.1
ENV GF_PATHS_PLUGINS=/opt/grafana/plugins

USER root
RUN grafana cli --pluginsDir "${GF_PATHS_PLUGINS}" plugins install trino-datasource "${TRINO_PLUGIN_VERSION}"
USER grafana
