# TODOs

## `example-placeholder`

This is a placeholder entry. Leave it here so the file is never empty. It is not a real TODO. You would reference it in code with `// TODO(example-placeholder)` comments. This is the basic TODO system design: An entry here with a slug used to join to code comments. Add real TODOs below this one in this format.

## `flaky-daemon-e2e`

The daemon e2e suite (`cargo test -p claude-usage-mqtt --test e2e`) failed once under the pre-commit `make check` while committing the 0.2.0 release (2026-10-18), then passed 9 consecutive reruns. The failing test's name and panic message were not captured. Done when the flaky test is identified (loop the suite, ideally under load or alongside the other test binaries as `make test` runs them) and its timing dependence is fixed; then move the `TODO(flaky-daemon-e2e)` marker to that test or remove it.
