#!/bin/bash
#
# Initialize the Mill dir ($MILL_DIR or /$MANTA_USER/stor/mill)
# layout (e.g. assets, base dirs) in Manta.
#
# TODO: should this be 'milld init'? Only really want *one* coordinating
# admin to init that -- not each milld agent. Perhaps eventually a
# milladm init.
#

set -o errexit
TOP=$(cd $(dirname $0)/../; pwd)


if [[ -z "$MILL_DIR" ]]; then
    MILL_DIR=/$MANTA_USER/stor/mill
fi
echo "Initializing Mill dir ($MILL_DIR) in Manta"

mmkdir $MILL_DIR/logs
mmkdir $MILL_DIR/assets
mput -f $TOP/assets/tlog.js $MILL_DIR/assets/tlog.js
mput -f $TOP/assets/ilog.js $MILL_DIR/assets/ilog.js
mput -f $TOP/assets/search.js $MILL_DIR/assets/search.js
