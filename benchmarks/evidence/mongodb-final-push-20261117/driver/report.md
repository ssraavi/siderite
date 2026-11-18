
BENCHMARK RESULTS SUMMARY

### Database-backed Todo API

| Test | Median req/s (S / F) | Paired ratio (95% CI) | p99 ms (S / F) | Adoption / Strong lead |
|---|---|---|---|---|
| `mongodb: list 20` | 11,521 / 1,178 | 9.833x (9.559-10.095) | 7.827 / 56.351 | pass / pass |
| `mongodb: get one` | 13,266 / 1,228 | 10.904x (10.747-11.113) | 6.686 / 55.065 | pass / pass |
| `mongodb: insert` | 6,125 / 1,125 | 5.419x (5.282-5.504) | 7.388 / 29.883 | pass / pass |

Adoption: paired point >=1.10x, lower bound >parity. Strong lead: lower bound >1.10x. Both require >=5 identified pairs, every timed trial >=10 s, and each paired p99 ratio <=1.05.
Intervals describe the measured host and workload; they do not establish a lead across all deployments.

