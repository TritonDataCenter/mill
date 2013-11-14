- fold 'merge-cat' into 'cat' (impl detail)
- test with larger corpus
- multi-level 'foo.bar' support for 'mill search' queries and for
  'mill search -o foo.bar' tabular output columns
- ensure tlog and ilog processing works on logs with non-timestamp lines
- archive loading
- milld add
- milld add nginx (with helpers for finding the log_format line)
- milld start/stop as intended ... but perhaps 'milld run' for *non* backgrounding
  for SMF??
- clean out 'search.js': can't do the tabular layout in search.js because then
  can't sort on time in a reduce step. Only using 'search.js -j ...' now.


# to discuss, todo later

- special case '.i' for bunyan logs to actually be a snaplink
- milld race on creating a tlog job for each source
- mill cat/grep: support '-l|--local' arg for interpreting start and end values in local time
- mill cat/grep: support svc= and inst= being substring matches
- milld source config: reasonable default for 'instance' (hostname or zonename)
  Perhaps support templates for those: $hostname, $zonename, etc.
- milld uploading is simplistic in that it does none of the timestamp parsing
  of the logs on upload.
  Problem: After a haitus of milld being down on a server or servers, the next
  upload is from a big timespan. The 'mill cat' et al result in a cat of a large
  time swath instead of a desired tight 5 minute slop.
  Potential Solution: Get the .tlog generation to rebalance on 5-minute
  slots.
- [Yunong?] Perhaps just have *one* uploader for all sources rather than one
  for each?
