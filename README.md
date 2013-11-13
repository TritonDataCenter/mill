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

    mill cat ...
    mill merge-cat ...
    mill grep ...


# Configuration

The config for `milld` and `mill` is a JSON file at "/etc/mill.json".
TODO: `~/.mill.json` for mill client config.

Variable        | Description
---             | ---
`manta`         | Manta auth info. Generally they fallback to the std "MANTA_" envvars.
`manta.url`     | The Manta URL. Defaults to `MANTA_URL` envvar.
`manta.account` | The Manta user with which to auth. Defaults to `MANTA_USER` envvar.
`manta.keyId`   | The ssh public key fingerprint with which to auth. Defaults to `MANTA_KEY_ID` envvar.
`manta.dataDir` | Mill's base dir in Manta. If not in the config file, it will use `MILL_DIR` from the env. By default this is "/$account/stor/mill",
