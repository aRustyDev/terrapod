# Woven image baseline

This branch starts from upstream TerraPod `v1.7.4` at `cfaf1a5fa397bf5b0bac85559affecab3e38ddaa`. The Woven workflow builds API, web, listener, runner and migrations from a single commit, publishes immutable digest references with OCI provenance, and uploads `images.json` for the chart handoff. Application fixes land in this fork and should be proposed upstream. The scheduled comparison workflow opens a draft sync PR after it is merged into the fork's default branch; it does not merge upstream automatically.
