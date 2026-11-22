# Pull-based deploy

The home machine redeploys itself from `main`. Every 5 minutes a systemd timer runs
[`pull-deploy.sh`](pull-deploy.sh), which fetches `origin/main` and, when there is a new
commit whose CI jobs (lint, test, security, dockerfiles) all passed, resets the clone to it and
runs `make deploy`. Nothing on GitHub can reach the machine, so a pull request from a fork
can't run code on it, which a self-hosted Actions runner would allow on a public repo.

A deploy lands up to 5 minutes after CI goes green. A failed `make deploy` is retried on
each tick until it succeeds or a newer commit arrives. Logs go to the journal:

```sh
journalctl --user -u flight-lakehouse-deploy -f
```

## Setup

Linux with systemd, Docker, git, curl, jq, make and uv. The user must be able to run
`docker` without sudo (the `docker` group).

1. Clone the repo to `~/flight-lakehouse` (or change `ExecStart` in the service), and copy in
   `.env` and `data/recordings/` from the old checkout. The Docker volumes are shared, since
   the compose project is always named `flight-lakehouse`.
2. While the repo is private, create a fine-grained token with read-only access to this repo
   (Contents and Checks) and put it in `~/.config/flight-lakehouse/deploy.env`:

   ```sh
   GITHUB_TOKEN=github_pat_...
   ```

   Once the repo is public it is optional; without it, the GitHub API allows 60 requests an
   hour, and each tick uses at most one.
3. Install and start the timer:

   ```sh
   mkdir -p ~/.config/systemd/user
   cp infra/deploy/flight-lakehouse-deploy.{service,timer} ~/.config/systemd/user/
   systemctl --user daemon-reload
   systemctl --user enable --now flight-lakehouse-deploy.timer
   loginctl enable-linger "$USER"   # keep user timers running when logged out
   ```

4. Run one deploy by hand and check it works:
   `systemctl --user start flight-lakehouse-deploy && journalctl --user -u flight-lakehouse-deploy -n 50`.

## Removing the old runner

Once the timer has deployed a commit, stop and unregister the self-hosted runner, from its
directory on the machine:

```sh
sudo ./svc.sh stop && sudo ./svc.sh uninstall
./config.sh remove --token <token from Settings → Actions → Runners → the runner → Remove>
```

Then check that no runner is listed under Settings → Actions → Runners.
