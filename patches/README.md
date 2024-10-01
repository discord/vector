This directory contains the Vector binary that we use. We use a non-standard binary to patch GCS retry behavior. In production, we noticed that we were getting a lot of 'Connection Reset by Peer' errors on the GCS sink, and in the GCS sink, these errors are *not* retriable.

We patch the GCS sink to be much more greedy in retrying, so that it pretty much retries anything.

## In This Directory
- `gcs-retry.path` -> A patch file which can be applied to the Vector main branch to introduce the retrying behavior we want


The Dockerfiles will compile a version of Vector with our patches in them automatically. Check those files for the commit hash that we are based off of in the case you'd like to make some updates.

To update the patch, clone the vector repo, checkout the specified commit hash and make your changes. After making your changes, run `git diff > gcs-retry.patch` to save the diff and copy it into this directory. The build files will build vector with your patch automatically.


### Currently Patched
The following are patched:
- Fixing GCS Sink error type that allows proper retry handling
- Extremely generous retry logic that functionaly retries everything
- Backport updated GCP auth token handling from https://github.com/vectordotdev/vector/pull/20574
