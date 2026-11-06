#!/bin/sh
# Optional: trust an extra CA certificate during image build steps that download things (pip, Maven).
# Needed behind a TLS-inspecting proxy (some university or company networks). Without the build secret
# `extra_ca` this script just runs the command, so normal builds are unaffected.
# The CA is trusted only for the command that follows; it is not installed into the image and
# the temporary files are deleted afterwards.
#
#   EXTRA_CA_FILE=/path/to/proxy-ca.pem docker compose --profile smoke build
set -eu
CA=/run/secrets/extra_ca
if [ -s "$CA" ]; then
  d=/tmp/extra-ca
  mkdir -p "$d"
  cat /etc/ssl/certs/ca-certificates.crt "$CA" > "$d/bundle.pem"
  cp "$JAVA_HOME/lib/security/cacerts" "$d/truststore.jks"
  chmod u+w "$d/truststore.jks"
  awk -v dir="$d" '/BEGIN CERTIFICATE/{n++} n>0{print > (dir "/ca-" n ".crt")}' "$CA"
  for f in "$d"/ca-*.crt; do
    keytool -importcert -noprompt -trustcacerts -keystore "$d/truststore.jks" -storepass changeit \
      -alias "extra-$(basename "$f")" -file "$f" >/dev/null
  done
  export PIP_CERT="$d/bundle.pem" SSL_CERT_FILE="$d/bundle.pem" REQUESTS_CA_BUNDLE="$d/bundle.pem"
  export JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=$d/truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"
  status=0
  "$@" || status=$?
  rm -rf "$d"                      # leave nothing behind in the image layer
  exit "$status"
fi
exec "$@"
