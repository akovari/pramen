# Security policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |
| < 0.1   | No        |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Email the maintainer privately with:

- A description of the issue and affected components
- Steps to reproduce or a proof of concept
- Impact assessment (data exposure, credential leakage, RCE, etc.)

You should receive an acknowledgment within a few business days. We will
coordinate disclosure and a fix before any public announcement.

## Scope notes

Pramen handles pipeline specs, credentials via environment variables, and
provider API traffic. Reports involving secrets appearing in normalized
plans, logs, or ledger exports are in scope. Cloud provider misconfiguration
in user-owned AWS accounts is generally out of scope unless caused by Pramen
emitting unsafe defaults.
