
BENCHMARK RESULTS SUMMARY

### Database-backed Todo API

| Test | Median req/s (S / F) | Paired ratio (95% CI) | p99 ms (S / F) | Adoption / Strong lead |
|---|---|---|---|---|
| `mongodb-siderite-native: list 20` | 14,923 / 1,008 | 14.906x (14.798-15.015) | 6.874 / 64.297 | pass / pass |
| `mongodb-siderite-native: get one` | 19,185 / 1,062 | 17.866x (17.373-18.387) | 5.487 / 60.781 | pass / pass |

Adoption: paired point >=1.10x, lower bound >parity. Strong lead: lower bound >1.10x. Both require >=5 identified pairs, every timed trial >=10 s, and each paired p99 ratio <=1.05.
Intervals describe the measured host and workload; they do not establish a lead across all deployments.

