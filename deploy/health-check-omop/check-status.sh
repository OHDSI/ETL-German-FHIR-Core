#!/bin/bash

if psql -U ohdsi_admin_user -d ohdsi -c "SELECT * FROM cds_cdm.load_status;" | grep -q "finished"; then
  exit 0  # Table exists, return healthy
else
  exit 1  # Table does not exist, return not healthy
fi
