#!/bin/sh
if [ "${NET_DELAY:-0ms}" != "0ms" ]; then
  tc qdisc add dev eth0 root netem delay "$NET_DELAY" "${NET_JITTER:-0ms}" distribution normal
fi
exec chord-node "$@"