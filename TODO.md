- [Trent?] .log -> .tlog job run post-upload: the "tlog" job
- [Yunong?] Perhaps just have *one* uploader for all sources rather than one
  for each?
- [Trent] fix 'mill cat' to work with the .tlog files
- [Isaac] .tlog -> .ilog processing to run post-upload
  I think it should derive from the *.tlog* because then we can re-run the
  .ilog generation after the .log files have been removed.
- mill merge-cat
- mill grep


# to discuss, todo later

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

