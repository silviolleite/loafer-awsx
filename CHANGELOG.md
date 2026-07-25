# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Releases are automated with [release-please](https://github.com/googleapis/release-please)
from the [Conventional Commits](https://www.conventionalcommits.org/) history.

## [Unreleased]

## [0.1.0] - 2024-01-01

### Added

- Initial release of `loafer-awsx`, a Go library for consuming and producing
  AWS SQS/SNS messages with routing, worker pools, visibility management,
  typed handlers, middleware (recovery, logging, Prometheus metrics,
  OpenTelemetry), ID generation, and observe-only DLQ signalling.
