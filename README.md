Tooling to upload and analyze logs in Manta.

# milld agent

Run `milld` on your servers to handle rotating and uploading your
logs to Manta.

Dev Notes (based on apachectl, FWIW):

    milld start
    milld stop
    milld restart
    milld status
    milld configtest

    milld add -t nginx /var/log/nginx.log
    milld add -t bunyan /var/log/myapp.log
    milld setup/config
        Interactively setup Manta info, and perhaps logs to handle.
        Q: setup a new SSH key for this? Probably want *one* key for multiple
        machines using the 'mill' role in Manta (once RBAC is there).


# mill client tool

Run the `mill` CLI to grep, analyze and get reports on your logs.

Dev Notes:

    mill grep ...



# mill Manta layout

Default `MILL_DIR` is `mill`, so:

    /$user/stor/$MILL_DIR/
        logs/
            [$dc?]/$name/$year/$month/$day/$hour/
                $node.log
        archive/...    # or something for backdated logs to integrate
