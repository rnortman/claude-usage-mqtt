# Development and release entry points.
#
# `make check` is the local gate the pre-commit hook runs: format, lint, the
# full test suite (including the end-to-end tests against a real mosquitto),
# and the scrub kit's self-check. CI runs `make rust-check`, the same minus the
# kit check. Nothing here ever touches api.anthropic.com — the e2e tests mock
# it in-process, on purpose.

.PHONY: check rust-check init-check scrub-tree fmt fmt-check clippy test build release release-musl install deploy clean publish-check

MUSL_TARGET := x86_64-unknown-linux-musl
BIN := claude-usage-mqtt

check: fmt-check clippy test init-check

rust-check: fmt-check clippy test

init-check:
	brenn-scrub init --check

scrub-tree:
	brenn-scrub tree

fmt:
	cargo fmt

fmt-check:
	cargo fmt --check

clippy:
	cargo clippy --locked --all-targets -- -D warnings

# The e2e tests spawn mosquitto. Locally they skip themselves if it is not
# installed or will not start, so a missing broker degrades to reduced
# coverage; under CI (the `CI` environment variable set) the same condition
# fails them. --no-fail-fast so one red test binary cannot hide the others.
test:
	cargo test --locked --workspace --no-fail-fast

build:
	cargo build -p $(BIN)

release:
	cargo build --release -p $(BIN)

# Static musl build: the deploy target may not share this box's glibc, and a
# single self-contained binary is the whole point of choosing Rust here.
release-musl:
	cargo build --release --target $(MUSL_TARGET) -p $(BIN)

# Install locally under systemd --user. The daemon runs on the machine where the
# Claude Code profiles live; it reaches the broker over the network. See README.
install: release
	install -Dm755 target/release/$(BIN) $(HOME)/.local/bin/$(BIN)
	install -Dm644 deploy/$(BIN).service $(HOME)/.config/systemd/user/$(BIN).service
	systemctl --user daemon-reload
	@echo "Installed. Configure $(HOME)/.config/$(BIN)/config.toml, then:"
	@echo "  systemctl --user enable --now $(BIN)"

# Push a musl build to a remote host over ssh. Requires DEPLOY_HOST.
deploy: release-musl
	@test -n "$(DEPLOY_HOST)" || { echo "set DEPLOY_HOST=user@host"; exit 1; }
	scp target/$(MUSL_TARGET)/release/$(BIN) $(DEPLOY_HOST):/tmp/$(BIN).new
	scp deploy/$(BIN).service $(DEPLOY_HOST):/tmp/$(BIN).service
	ssh $(DEPLOY_HOST) 'bash -s' < deploy/remote-install.sh

# Pre-publish gate: dry-run `cargo publish` for each crate in dependency order
# (core first, since the others depend on it). No crate is actually published.
publish-check:
	cargo publish --dry-run -p claude-usage-mqtt-core
	cargo publish --dry-run -p claude-usage-mqtt-client
	cargo publish --dry-run -p claude-usage-mqtt

clean:
	cargo clean
