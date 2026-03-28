# Security Policy

## Scope

This project is a read-only data visualisation tool. It fetches public data from
GitHub and generates a static HTML file — it has no user accounts, no database,
and no server-side components.

Relevant security concerns include:

- Data integrity issues (e.g. the upstream CSV being tampered with in a way that causes XSS in the generated HTML)
- Dependency vulnerabilities in `pandas`, `plotly`, or `requests`
- Unintended information disclosure in the generated output

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report privately by emailing the maintainer or opening a
[GitHub private security advisory](../../security/advisories/new).

Include:
- A description of the vulnerability and its potential impact
- Steps to reproduce or a proof-of-concept
- Any suggested fix if you have one

You can expect an acknowledgement within 72 hours and a resolution or mitigation
plan within 14 days for confirmed issues.

## Upstream Data

Alert data is sourced from [dleshem/israel-alerts-data](https://github.com/dleshem/israel-alerts-data),
a third-party public repository. This project does not control or verify the
integrity of that data beyond what is fetched at runtime.
