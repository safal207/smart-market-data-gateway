# Market-data provider licensing checklist

Complete this document before enabling a real provider outside an isolated technical test.

## Provider identification

- Provider and product:
- Exchange/data origin:
- Contract or terms version/date:
- Account type and environment:
- Legal owner of the account:
- Reviewer and approval date:

## Rights matrix

| Capability | Allowed | Restricted | Unknown | Evidence / clause |
|---|:---:|:---:|:---:|---|
| Temporary caching | ☐ | ☐ | ☐ | |
| Persistent historical storage | ☐ | ☐ | ☐ | |
| Display to authenticated end users | ☐ | ☐ | ☐ | |
| Redistribution through WebSocket | ☐ | ☐ | ☐ | |
| Redistribution through REST API | ☐ | ☐ | ☐ | |
| Commercial subscription tiers | ☐ | ☐ | ☐ | |
| Non-display / algorithmic use | ☐ | ☐ | ☐ | |
| Derived-data products | ☐ | ☐ | ☐ | |
| Public benchmark publication | ☐ | ☐ | ☐ | |
| Sharing sample payloads in an open repository | ☐ | ☐ | ☐ | |
| Use of delayed data | ☐ | ☐ | ☐ | |
| Use of real-time data | ☐ | ☐ | ☐ | |

## Operational constraints

- Authentication method and secret-storage requirements:
- Request, connection, and symbol limits:
- Required exchange attribution:
- Data-delay labels:
- Geographic restrictions:
- User-count or device-count reporting:
- Audit and deletion obligations:
- Incident-notification obligations:
- Prohibited use cases:

## Release gate

Commercial redistribution remains disabled until all unknown rights are resolved and an authorized reviewer signs off. Technical tests must use the minimum data scope, avoid publishing raw licensed payloads, redact credentials, and record the exact terms used for the test.
