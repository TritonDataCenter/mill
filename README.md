Tooling to upload and analyze logs in Manta.

# milld agent

Run `milld` on your servers to handle rotating and uploading your
logs to Manta.

1. Edit milld config file (TODO: 'milld setup' to interactively do this and
   'milld config' to interact with the mill JSON config file.)

        mkdir -p /var/db/mill/millconfig.json
        vi /var/db/mill/millconfig.json

2. Tell milld to handle some log files.

        milld add -t nginx /var/log/nginx.log
        milld add -t bunyan /var/log/myapp.log

   Milld will "own" these log files. IOW it will handle rotating and uploading.

3. Start the daemon

        milld start


# mill client tool

Run the `mill` CLI to grep, analyze and get reports on your logs.

Dev Notes:

    mill grep ...
    mill cat ...

Examples:

    mill cat service=NAME1 inst=ID start=DATE end=DATE-OR-RANGE
    mill merge-cat [service=NAME1 service=NAME2 ...] [inst=ID1 inst=ID2...] start=DATE end=DATE-OR-RANGE
    mill grep [service=NAME1 service=NAME2 ...] [inst=ID1 inst=ID2...] start=DATE end=DATE-OR-RANGE SEARCH-TERM
        # (optional?) Does the merge on time if multiple service or inst.
    mill cat service=NAME inst=ID start=DATE end=DATE-OR-RANGE
        # default start is 10m ago, default end=5m  ("latest") ... kinda 'mill tail'
    mill ls [service=NAME service=NAME2] inst=ID ...
        # all services
        # all instances
        # date ranges with logs for a service/instance
        sugar



# mill Manta layout

Default `MILL_DIR` is `mill`, so:

    /$user/stor/$MILL_DIR/
        logs/
            [$dc?]/$service/$year/$month/$day/$hour/
                $node.log
                $node-$subhourlyperiod.log
        archive/...    # or something for backdated logs to integrate

Notes:

    /trentm/stor/mill
        logs/
            nginx/2013/11/11/12/
                # Upload 5 minute chunks to  allow working with smaller
                # files for 'mill cat' etc. This might be a tunable at some
                # point.
                foo-05.log
                foo-10.log
                foo-15.log
                foo-20.log
                ...
                foo-60.log
        backfill/...    # TODO: or something for backdated logs to integrate

# Configuration
