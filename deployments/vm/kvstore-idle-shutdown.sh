#!/usr/bin/env bash
# Powers the demo VM off once nobody has used the demo for IDLE_LIMIT
# seconds. Run every 5 minutes by kvstore-idle-shutdown.timer; the launcher
# (cmd/launcher) starts the VM again on the next visit.
#
# "Used" means inbound packets on the public client ports 8081-8083, counted
# by an iptables rule this script maintains. Any packet counts, not just new
# connections, because an open dashboard polls over keep-alive connections.
# MIN_PACKETS filters out the odd internet port scan, which would otherwise
# keep the VM awake forever.
set -euo pipefail

IDLE_LIMIT=${IDLE_LIMIT:-1800}  # seconds without real traffic before shutdown
MIN_PACKETS=${MIN_PACKETS:-100} # packets per check that count as activity
STATE_DIR=/var/lib/kvstore-idle
STATE=$STATE_DIR/state
RULE=(-p tcp -m multiport --dports 8081:8083 -m comment --comment kvstore-demo-traffic)

mkdir -p "$STATE_DIR"
iptables -t mangle -C PREROUTING "${RULE[@]}" 2>/dev/null || iptables -t mangle -I PREROUTING "${RULE[@]}"

count=$(iptables -t mangle -L PREROUTING -v -x -n | awk '/kvstore-demo-traffic/ {print $1; exit}')
now=$(date +%s)
boot_id=$(cat /proc/sys/kernel/random/boot_id)

prev_boot="" prev_count=0 last_active=$now
if [[ -f $STATE ]]; then
  read -r prev_boot prev_count last_active <"$STATE"
fi

# A counter lower than last time means the rule was recreated from zero.
delta=$(( count >= prev_count ? count - prev_count : count ))

if [[ $prev_boot != "$boot_id" ]]; then
  # First check since boot: the VM was just woken up for a reason, so treat
  # boot as activity (this also gives the first visitor time to arrive).
  last_active=$now
elif (( delta >= MIN_PACKETS )); then
  last_active=$now
fi

echo "$boot_id $count $last_active" >"$STATE"

idle=$(( now - last_active ))
if (( idle >= IDLE_LIMIT )); then
  logger -t kvstore-idle "no demo traffic for ${idle}s, powering off"
  systemctl poweroff
fi
