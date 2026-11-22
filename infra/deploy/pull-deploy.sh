#!/usr/bin/env bash
# Pull-based deploy for the home machine: if origin/main has a new commit and its CI is
# green, reset this clone to it and run `make deploy`. A systemd timer runs it every few
# minutes (see infra/deploy/README.md), so GitHub never runs code on this machine.
#
# Needs git, curl and jq. GITHUB_TOKEN is optional on a public repo (it lifts the API rate
# limit) and required while the repo is private.
set -euo pipefail

REPO=javigarlop/flight-lakehouse
REQUIRED_JOBS=(lint test security) # CI jobs that must have passed, as named in ci.yml
REPO_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
STATE_DIR=${STATE_DIRECTORY:-${XDG_STATE_HOME:-$HOME/.local/state}/flight-lakehouse}
STATE_FILE=$STATE_DIR/deployed-sha

log() { echo "pull-deploy: $*"; }

# Prints "green", "pending" or "failed" for the commit's GitHub check runs.
ci_status() {
	local sha=$1 auth=()
	[[ -n ${GITHUB_TOKEN:-} ]] && auth=(-H "Authorization: Bearer $GITHUB_TOKEN")
	curl -fsS "${auth[@]}" -H "Accept: application/vnd.github+json" \
		"https://api.github.com/repos/$REPO/commits/$sha/check-runs?per_page=100" |
		jq -r --args '
			.check_runs as $runs
			| if any($runs[]; .status == "completed"
				and (.conclusion | IN("success", "skipped", "neutral") | not)) then "failed"
			elif any($runs[]; .status != "completed") then "pending"
			# Jobs not created yet look like a green commit with no runs, so require them.
			elif all($ARGS.positional[]; . as $job | any($runs[]; .name == $job)) then "green"
			else "pending" end
		' "${REQUIRED_JOBS[@]}"
}

main() {
	cd "$REPO_DIR"
	mkdir -p "$STATE_DIR"

	# One deploy at a time; a timer tick during a long deploy just exits.
	exec 9>"$STATE_DIR/lock"
	flock -n 9 || { log "a deploy is already running"; return 0; }

	git fetch --quiet origin main
	local target deployed
	target=$(git rev-parse origin/main)
	deployed=$(cat "$STATE_FILE" 2>/dev/null || true)
	[[ $target == "$deployed" ]] && return 0

	case $(ci_status "$target") in
	green) ;;
	pending) log "CI still running for ${target:0:7}"; return 0 ;;
	*) log "CI failed or its status is unavailable for ${target:0:7}; not deploying"; return 0 ;;
	esac

	log "deploying ${target:0:7} (was ${deployed:0:7})"
	# Ignored files survive the reset: .env and data/recordings/ stay in place.
	git reset --hard --quiet "$target"
	uv sync --locked
	make deploy
	echo "$target" >"$STATE_FILE"
	log "deployed ${target:0:7}"
}

# Bash reads a script as it runs it, and the reset above can rewrite this file. Calling
# main on the last line makes bash parse the whole file before anything changes it.
main "$@"; exit
