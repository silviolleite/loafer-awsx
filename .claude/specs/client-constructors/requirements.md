# Requirements Document

## Introduction

Today, applications that use `loafer-awsx` must import the AWS SDK for Go v2 service
packages (`sqs`, `sns`, `scheduler`) directly to construct the service clients that the
broker, producer, and consumer components require. This leaks the AWS SDK dependency into
consumer code and prevents `loafer-awsx` from acting as a complete wrapper over the SDK.

This feature adds first-class client constructors to the library for the AWS SQS, SNS, and
EventBridge Scheduler services. Each constructor builds a ready-to-use service client from
an existing AWS configuration (as produced by the `conn` package) and, during
construction, validates connectivity with a "Ping": a lightweight, read-only AWS request
that verifies the client can reach its AWS service with valid credentials. The Ping is an
internal step of construction, not a caller-invoked operation; when it fails the
constructor returns an error instead of a client. The Ping uses a dedicated timeout and
retry budget that are independent of the client's general request retry policy, with
library defaults of a 3-second timeout and a maximum of 2 retries. Callers may override
these Ping settings through functional options and may disable the validation entirely for
environments where the read-only permission is not granted or where offline construction
is required.

By owning client construction and connectivity validation, `loafer-awsx` becomes a
complete wrapper over AWS SDK for Go v2, so that a future migration of the underlying SDK
version remains transparent to the applications that depend on the library.

## Glossary

- **Loafer_AWSX**: The `loafer-awsx` Go library that wraps AWS SDK for Go v2.
- **Caller**: An application or package that depends on Loafer_AWSX to construct AWS
  service clients.
- **AWS_Config**: The `aws.Config` value produced by the existing `conn` package, carrying
  region, credentials, endpoint, and retry settings.
- **SQS_Client_Constructor**: The Loafer_AWSX function that builds an SQS service client.
- **SNS_Client_Constructor**: The Loafer_AWSX function that builds an SNS service client.
- **Scheduler_Client_Constructor**: The Loafer_AWSX function that builds an EventBridge
  Scheduler service client.
- **Service_Client**: A constructed SQS, SNS, or Scheduler client returned by a
  Client_Constructor.
- **Context**: The `context.Context` a Caller passes to a Client_Constructor, used to bound
  and cancel the connectivity validation.
- **Connectivity_Validation**: The Ping step performed by a Client_Constructor during
  construction that verifies the Service_Client can reach its AWS service with valid
  credentials by issuing a lightweight, read-only AWS request.
- **Ping_Timeout**: The maximum total duration allowed for the Connectivity_Validation,
  including its retries. Default value is 3 seconds.
- **Ping_Retry_Limit**: The maximum number of retry attempts the Connectivity_Validation
  performs for a failed Ping request, beyond the initial attempt. Default value is 2.
- **Client_Option**: A functional option, following the pattern used by the existing `conn`
  package, that customizes a Client_Constructor.
- **ErrInvalidOption**: The existing sentinel error returned when a functional option
  receives an invalid value.
- **ErrPingFailed**: The sentinel error returned when the Connectivity_Validation does not
  succeed within its timeout and retry budget.

## Requirements

### Requirement 1: Construct an SQS client without importing the AWS SDK

**User Story:** As a Caller, I want to construct an SQS client through Loafer_AWSX, so that
I do not need to import the AWS SDK service packages in my application code.

#### Acceptance Criteria

1. WHEN a Caller invokes the SQS_Client_Constructor with a Context and a valid AWS_Config and the Connectivity_Validation succeeds or is disabled, THE Loafer_AWSX SHALL return an SQS Service_Client that satisfies the SQS client interface required by the broker and consumer components.
2. THE SQS Service_Client returned by the SQS_Client_Constructor SHALL be usable without the Caller importing the AWS SDK `sqs` package.
3. WHERE a Caller supplies Client_Options to the SQS_Client_Constructor, THE SQS_Client_Constructor SHALL apply each option in the order supplied.
4. IF a Client_Option supplied to the SQS_Client_Constructor returns an error, THEN THE SQS_Client_Constructor SHALL return an error that wraps ErrInvalidOption and SHALL NOT return a Service_Client.
5. WHEN the SQS_Client_Constructor returns, THE SQS_Client_Constructor SHALL return either a Service_Client or an error, and never neither.

### Requirement 2: Construct an SNS client without importing the AWS SDK

**User Story:** As a Caller, I want to construct an SNS client through Loafer_AWSX, so that
I can publish messages without importing the AWS SDK service packages.

#### Acceptance Criteria

1. WHEN a Caller invokes the SNS_Client_Constructor with a Context and a valid AWS_Config and the Connectivity_Validation succeeds or is disabled, THE Loafer_AWSX SHALL return an SNS Service_Client that satisfies the SNS client interface required by the producer component.
2. THE SNS Service_Client returned by the SNS_Client_Constructor SHALL be usable without the Caller importing the AWS SDK `sns` package.
3. WHERE a Caller supplies Client_Options to the SNS_Client_Constructor, THE SNS_Client_Constructor SHALL apply each option in the order supplied.
4. IF a Client_Option supplied to the SNS_Client_Constructor returns an error, THEN THE SNS_Client_Constructor SHALL return an error that wraps ErrInvalidOption and SHALL NOT return a Service_Client.
5. WHEN the SNS_Client_Constructor returns, THE SNS_Client_Constructor SHALL return either a Service_Client or an error, and never neither.

### Requirement 3: Construct an EventBridge Scheduler client without importing the AWS SDK

**User Story:** As a Caller, I want to construct an EventBridge Scheduler client through
Loafer_AWSX, so that I can enable scheduled retries without importing the AWS SDK service
packages.

#### Acceptance Criteria

1. WHEN a Caller invokes the Scheduler_Client_Constructor with a Context and a valid AWS_Config and the Connectivity_Validation succeeds or is disabled, THE Loafer_AWSX SHALL return a Scheduler Service_Client that satisfies the Scheduler client interface required by the consumer component.
2. THE Scheduler Service_Client returned by the Scheduler_Client_Constructor SHALL be usable without the Caller importing the AWS SDK `scheduler` package.
3. WHERE a Caller supplies Client_Options to the Scheduler_Client_Constructor, THE Scheduler_Client_Constructor SHALL apply each option in the order supplied.
4. IF a Client_Option supplied to the Scheduler_Client_Constructor returns an error, THEN THE Scheduler_Client_Constructor SHALL return an error that wraps ErrInvalidOption and SHALL NOT return a Service_Client.
5. WHEN the Scheduler_Client_Constructor returns, THE Scheduler_Client_Constructor SHALL return either a Service_Client or an error, and never neither.

### Requirement 4: Validate connectivity during construction

**User Story:** As a Caller, I want the constructor to verify that the client can reach its
AWS service with valid credentials before returning it, so that I detect configuration and
connectivity problems at construction time rather than on the first consume or publish.

#### Acceptance Criteria

1. WHEN a Client_Constructor performs the Connectivity_Validation, THE Client_Constructor SHALL issue a lightweight, read-only AWS request appropriate to the Service_Client's service to confirm connectivity and credential validity.
2. IF the Connectivity_Validation receives a successful response within the Ping_Timeout and Ping_Retry_Limit, THEN THE Client_Constructor SHALL return the Service_Client and no error.
3. IF the Connectivity_Validation does not receive a successful response after exhausting the Ping_Retry_Limit, THEN THE Client_Constructor SHALL return an error that wraps ErrPingFailed and the underlying cause and SHALL NOT return a Service_Client.
4. WHEN a Client_Constructor performs the Connectivity_Validation, THE Client_Constructor SHALL derive from the Context a child context bounded by the Ping_Timeout for the validation.
5. IF the Context is canceled before the Connectivity_Validation completes, THEN THE Client_Constructor SHALL stop scheduling further Ping attempts and return an error that wraps ErrPingFailed and reports the cancellation and SHALL NOT return a Service_Client, WHILE allowing any in-flight AWS request to complete and report its result.

### Requirement 5: Apply default Ping timeout and retry budget

**User Story:** As a Caller, I want sensible default Ping settings, so that connectivity
validation works out of the box without extra configuration.

#### Acceptance Criteria

1. WHERE a Caller does not supply a Ping_Timeout option, THE Client_Constructor SHALL perform the Connectivity_Validation with a Ping_Timeout of 3 seconds.
2. WHERE a Caller does not supply a Ping_Retry_Limit option, THE Client_Constructor SHALL perform the Connectivity_Validation with a Ping_Retry_Limit of 2 retries.
3. THE Client_Constructor SHALL apply the Ping_Retry_Limit independently of the request retry count configured on the AWS_Config.

### Requirement 6: Override or disable connectivity validation through options

**User Story:** As a Caller, I want to override the default Ping timeout and retry budget
and to disable the validation entirely, so that I can tune connectivity validation for my
environment or skip it when my credentials lack the read-only permission or I need offline
construction.

#### Acceptance Criteria

1. WHERE a Caller supplies a Ping_Timeout option with a positive duration, THE Client_Constructor SHALL perform the Connectivity_Validation with the supplied Ping_Timeout instead of the default.
2. IF a Caller supplies a Ping_Timeout option with a duration less than or equal to zero, THEN THE Client_Constructor SHALL return an error that wraps ErrInvalidOption and SHALL NOT return a Service_Client.
3. WHERE a Caller supplies a Ping_Retry_Limit option, THE Client_Constructor SHALL perform the Connectivity_Validation with the supplied Ping_Retry_Limit instead of the default.
4. WHERE a Caller supplies the disable-Connectivity_Validation option, THE Client_Constructor SHALL return the Service_Client without issuing any read-only AWS request and without contacting the AWS service.

### Requirement 7: Preserve compatibility with existing components

**User Story:** As a maintainer, I want the new constructors to integrate with the existing
broker, producer, and consumer components, so that current wiring continues to work.

#### Acceptance Criteria

1. THE SQS Service_Client returned by the SQS_Client_Constructor SHALL be accepted by the broker and consumer components without additional adaptation.
2. THE SNS Service_Client returned by the SNS_Client_Constructor SHALL be accepted by the producer component without additional adaptation.
3. THE Scheduler Service_Client returned by the Scheduler_Client_Constructor SHALL be accepted by the consumer component for scheduled retries without additional adaptation.
4. THE Loafer_AWSX SHALL continue to expose the existing `conn` package configuration API unchanged.
