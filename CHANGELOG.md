# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Releases are automated with [release-please](https://github.com/googleapis/release-please)
from the [Conventional Commits](https://www.conventionalcommits.org/) history.

## [0.5.0](https://github.com/silviolleite/loafer-awsx/compare/v0.4.0...v0.5.0) (2026-08-02)


### Features

* **client:** add SQS, SNS, and scheduler client constructors ([3fddc41](https://github.com/silviolleite/loafer-awsx/commit/3fddc41283f7c6fcebee508e6e5376e4aacebbe1))
* **errors:** add ErrPingFailed sentinel error ([f1fe041](https://github.com/silviolleite/loafer-awsx/commit/f1fe0413f7f0ce84133f11c0454d2c17acff5537))

## [0.4.0](https://github.com/silviolleite/loafer-awsx/compare/v0.3.0...v0.4.0) (2026-07-28)


### Features

* **consumer:** add FIFO scheduled retry model ([f52c12c](https://github.com/silviolleite/loafer-awsx/commit/f52c12ca4ebf22db717ca4a9139594dfeadd00dc))
* **errors:** add scheduled-retry sentinel errors ([a19fc2b](https://github.com/silviolleite/loafer-awsx/commit/a19fc2bfb47a15f2edbe566d11112535cf94497c))
* **examples:** add scheduled retry example, provisioning, and local scheduler ([3ee45c7](https://github.com/silviolleite/loafer-awsx/commit/3ee45c7f1734765af27b362ac98b5686e4ef2253))
* **middleware:** expose native SQS user message attributes to handlers ([ddad86d](https://github.com/silviolleite/loafer-awsx/commit/ddad86dadfefd631d83cf56462a489ea7771879b))
* **router:** add per-route scheduled retry model configuration ([f06a379](https://github.com/silviolleite/loafer-awsx/commit/f06a3798c04b6a2e13103c86ccaa4ba78357a3cf))

## [0.3.0](https://github.com/silviolleite/loafer-awsx/compare/v0.2.0...v0.3.0) (2026-07-26)


### Features

* require Go 1.26 as the minimum supported version ([a9a8bee](https://github.com/silviolleite/loafer-awsx/commit/a9a8beede2cdb5ac8458e97de1bd22694b6a24b2))

## [0.2.0](https://github.com/silviolleite/loafer-awsx/compare/v0.1.0...v0.2.0) (2026-07-25)


### Features

* **broker:** add broker orchestrating consumers with unbounded shutdown wait by default ([f0c46dc](https://github.com/silviolleite/loafer-awsx/commit/f0c46dcc410499150588582a0cdab7f842741beb))
* **conn:** add AWS config builder with functional options ([b176627](https://github.com/silviolleite/loafer-awsx/commit/b176627f31d873649b6b59470ade3ff5ea7bac76))
* **consumer:** add SQS consumer with worker pool, visibility, and DLQ observability ([c576060](https://github.com/silviolleite/loafer-awsx/commit/c576060ad3876316ced872ee25a0aba22679b2d2))
* **errors:** add sentinel errors and Wrap helper ([e7c2c99](https://github.com/silviolleite/loafer-awsx/commit/e7c2c99b01884889f26add3d35b18c70ac845ea1))
* **idgen:** add key-based, random, and composite ID generators ([c5c0d9d](https://github.com/silviolleite/loafer-awsx/commit/c5c0d9de6fa944210efa282f56c242749f840b30))
* **logger:** add slog-based stdout and no-op loggers ([ebea57b](https://github.com/silviolleite/loafer-awsx/commit/ebea57b1eb0d8d929254f6eb9b358d24866f2249))
* **middleware:** add handler, message, and chain core types ([b9c7ea3](https://github.com/silviolleite/loafer-awsx/commit/b9c7ea3693787fdbf357cb9c32674e5a75fc68c2))
* **middleware:** add recovery, logging, metrics, and otel middlewares ([11a59f4](https://github.com/silviolleite/loafer-awsx/commit/11a59f41922625b6463693bc8219cc5bb8623132))
* **producer:** add SNS producer with single, batch, and ID generation ([23d0830](https://github.com/silviolleite/loafer-awsx/commit/23d083075d26e74a9a153a300d39a61ff0474142))
* **router:** add routes with run modes and observe-only DLQ config ([31f67f9](https://github.com/silviolleite/loafer-awsx/commit/31f67f9171be09472c3da1961e8279bf0ed907f2))
* **typed:** add codec, handler adapter, and typed producer ([d30c39b](https://github.com/silviolleite/loafer-awsx/commit/d30c39b484bebc0443bd94f0e23b70823aec4cbb))

## [Unreleased]

## [0.1.0] - 2024-01-01

### Added

- Initial release of `loafer-awsx`, a Go library for consuming and producing
  AWS SQS/SNS messages with routing, worker pools, visibility management,
  typed handlers, middleware (recovery, logging, Prometheus metrics,
  OpenTelemetry), ID generation, and observe-only DLQ signalling.
