This directory contains the Vector binary that we use. We use a non-standard binary to patch GCS retry behavior. In production, we noticed that we were getting a lot of 'Connection Reset by Peer' errors on the GCS sink, and in the GCS sink, these errors are *not* retriable.

We patch the GCS sink to be much more greedy in retrying, so that it pretty much retries anything.

## In This Directory
- `gcs-retry.patch` -> A patch file which can be applied to the Vector main branch to introduce the retrying behavior we want


The Dockerfiles will compile a version of Vector with our patches in them automatically. Check those files for the commit hash that we are based off of in the case you'd like to make some updates.

To update the patch, clone the vector repo, checkout the specified commit hash and make your changes. After making your changes, run `git diff > gcs-retry.patch` to save the diff and copy it into this directory. The build files will build vector with your patch automatically.

Update: as of Oct 23 2024 (commit 0eaefd6a1476b6e2b8d46d411dcbcb9eeda4e9c3 in the post-git-rewrite monorepo), we no longer use the `gcs-retry.patch` file in our Dockerfile to build the Vector image, as we've moved to a fork of the Vector repository. Instead, we checkout a specific commit from a branch on the forked Vector repo by default, with an env var that can override the branch to be checked out. See the `discord_data/vector_base/Dockerfile` file in the monorepo.

### Currently Patched
The following are patched:
- Fixing GCS Sink error type that allows proper retry handling
- Extremely generous retry logic that functionaly retries everything
- Backport updated GCP auth token handling from https://github.com/vectordotdev/vector/pull/20574
- Retry the GCS sink healthcheck, every 5 seconds, up to 3 times, stopping on the first success. Prints the healthcheck response (if not successful) to logs on each attempt for debugging
