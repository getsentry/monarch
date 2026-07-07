#!/bin/bash
# Allow the standby container to connect for physical replication (demo-only trust).
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
