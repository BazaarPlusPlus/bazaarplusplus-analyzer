# Analyzer Consumer Data Contract

## Purpose

The analyzer publishes two current snapshots for two independent consumers:

- the Web site reads seven days of additive hero metrics;
- the Mod reads the current build recommendation corpus.

The public object set is exactly:

```text
analyzer-v5/
├── heroes/latest.json
└── builds/latest.json
```

Both objects are mutable snapshots. A successful calculation overwrites the
corresponding object at the same key. There are no public releases, manifests,
daily partitions, quality files, or historical copies.

Operational download and participation counts belong to the run report, not
to either consumer payload.

## Shared source rules

### Accepted facts

A Run enters the fact layer only when all of the following are true:

- its hero is in the canonical hero catalog;
- its final rank is present and in the canonical rank catalog.

If either condition fails, the Run and all of its associated Battles and card
snapshots are discarded before facts are written. Therefore the analyzed
population has no unknown-rank segment and always satisfies:

```text
all = legend + non_legend
```

`legend` means final rank Legend. `non_legend` means any other recognized
final rank. The segment of every Battle and Matchup is inherited from its
owning Run.

### Complete days and windows

A Complete Source Day has 24 accepted Source Hours, from `00` through `23`
UTC. Both products use the latest seven consecutive Complete Source Days.

If a newer Source Day has a missing hour, an unresolved download failure, or
a failed fact validation, that day is not complete. The analyzer keeps the
existing public snapshot until a complete seven-day window can be calculated.
It never publishes a partial day.

`window.start` and `window.end` are inclusive UTC dates. `generated_at` is an
ISO-8601 UTC timestamp describing when the snapshot was produced.

## `heroes/latest.json`

### Role

This object contains seven daily partitions inside one response. The Web site
folds the daily integer counts to produce its 1-day, 3-day, and 7-day views.
Rates and averages are never stored because they cannot be merged safely.

Only `legend` and `non_legend` rows are stored. The Web site's `all` view is a
field-wise sum of those two rows. Every day contains one row for every
canonical hero and stored segment; a hero with no observations has zero
counts and an empty `matchups` array.

### Shape

```json
{
  "schema_version": 1,
  "kind": "hero_metrics",
  "generated_at": "2026-08-12T02:00:00Z",
  "window": {
    "start": "2026-08-05",
    "end": "2026-08-11",
    "days": 7
  },
  "days": [
    {
      "day": "2026-08-11",
      "rows": [
        {
          "hero": "Vanessa",
          "segment": "legend",
          "runs": {
            "completed": 16536,
            "scored": 16536,
            "ten_win": 6569
          },
          "outcomes": {
            "perfect": 579,
            "gold": 5990,
            "silver": 3505,
            "bronze": 3654
          },
          "ten_win_days": {
            "known_count": 6569,
            "sum_days": 82768
          },
          "matchups": [
            {
              "opponent_hero": "Jules",
              "decided": 2287,
              "wins": 1418,
              "losses": 869
            }
          ]
        }
      ]
    }
  ]
}
```

### Run fields

`runs.completed` counts completed Accepted Runs.

`runs.scored` counts completed Accepted Runs whose outcome can be classified
from victories and losses. The outcome buckets are:

| Bucket | Definition |
| --- | --- |
| `perfect` | 10 victories and 0 losses |
| `gold` | 10 victories and at least 1 loss |
| `silver` | 7–9 victories |
| `bronze` | 4–6 victories |
| misfortune | 0–3 victories; derived, not stored |

`runs.ten_win` is `perfect + gold`.

The stored outcome counts satisfy:

```text
0 <= scored <= completed
ten_win = perfect + gold
misfortune = scored - perfect - gold - silver - bronze
```

`ten_win_days.known_count` counts Ten-Win Runs with a known final Run day.
`ten_win_days.sum_days` is the sum of those final Run days. Both values are
stored so an average remains correct after combining multiple Source Days.

### Matchups

`matchups` retains hero-versus-hero Battle outcomes. It includes only decided
Battles whose opponent hero is canonical and whose `winner_side` is `player`
or `opponent`.

For every Matchup row:

```text
decided = wins + losses
matchup_win_rate = wins / decided
```

The removed per-game-day Battle view has no field in this contract. In
particular, `battle_days` must not be emitted.

### Web calculations

The Web site first selects the most recent 1, 3, or 7 entries in `days`, then
sums fields by hero and requested segment.

| Display value | Calculation |
| --- | --- |
| Ten-Win rate | `ten_win / completed` |
| Completed Runs | `completed` |
| Run share | hero `completed / sum(completed)` in the same scope |
| Ten-Win Runs | `ten_win` |
| Average Ten-Win final day | `sum_days / known_count` |
| Perfect rate | `perfect / scored` |
| Gold rate | `gold / scored` |
| Silver rate | `silver / scored` |
| Bronze rate | `bronze / scored` |
| Misfortune rate | derived `misfortune / scored` |
| Matchup win rate | `wins / decided` |
| Seven-day trend | daily `ten_win / completed` |

A division with a zero denominator produces `null`, not zero.

## `builds/latest.json`

### Role

This object is the Mod's latest build recall and recommendation corpus. It is
calculated over all Accepted Runs in the Analysis Window; it is not split by
rank segment.

The wire format remains schema-driven and compact. Build IDs are implicit
zero-based indices into each hero's `builds` array. `card_index` maps a card
reference to the Build IDs containing that card.

### Shape

```json
{
  "schema_version": 2,
  "kind": "ten_win_builds",
  "generated_at": "2026-08-12T02:00:00Z",
  "window": {
    "start": "2026-08-05",
    "end": "2026-08-11",
    "days": 7
  },
  "cards": [
    "11111111-1111-1111-1111-111111111111",
    "22222222-2222-2222-2222-222222222222",
    "33333333-3333-3333-3333-333333333333",
    "44444444-4444-4444-4444-444444444444",
    "55555555-5555-5555-5555-555555555555"
  ],
  "enchantments": [
    null,
    "Burn",
    "Shielded"
  ],
  "schemas": {
    "build": [
      "card_refs",
      "layout",
      "stats"
    ],
    "layout": [
      "card_ref",
      "slot",
      "tier",
      "enchant_ref",
      "size"
    ],
    "stats": [
      "completed_run_count",
      "ten_win_run_count",
      "ten_win_rate_bps",
      "p75_ten_win_final_day",
      "score"
    ]
  },
  "heroes": {
    "Vanessa": {
      "builds": [
        [
          [0, 1, 2, 3, 4],
          [
            [0, 0, 4, 0, 2],
            [1, 2, 3, 1, 2],
            [2, 4, 4, 0, 2],
            [3, 6, 3, 0, 2],
            [4, 8, 4, 2, 2]
          ],
          [123, 45, 3659, 13, 421037]
        ]
      ],
      "card_index": [
        [0, [0]],
        [1, [0]],
        [2, [0]],
        [3, [0]],
        [4, [0]]
      ]
    }
  }
}
```

`enchantments[0]` is always `null`, so layout rows use enchantment reference
zero for no enchantment. Card and enchantment tables are sorted
deterministically. `heroes` contains every canonical hero; a hero with no
selected Build has empty `builds` and `card_index` arrays.

The positional rows in the example decode as:

```text
build = [card_refs, layout, stats]
layout item = [card_ref, slot, tier, enchant_ref, size]
stats = [completed_run_count, ten_win_run_count,
         ten_win_rate_bps, p75_ten_win_final_day, score]
```

`ten_win_rate_bps` is rounded to the nearest basis point, so `3659` means
`36.59%`.

### Eligible builds

A completed Accepted Run contributes a final layout only when:

- it has exactly one final Battle;
- the Run's `final_battle_id` identifies that Battle;
- the player-hand item snapshot is present;
- every card has a positive size and a valid template identifier;
- the final board occupies exactly 10 slots.

The Build Identity is the hero plus the sorted multiset of final-board card
template identifiers. Position, tier, and enchantment do not participate in
identity. All eligible completed runs with the same identity contribute to
`completed_run_count`; runs with ten victories contribute to
`ten_win_run_count`.

A Build Candidate must have at least one Ten-Win Run. Its Representative
Layout is the most frequently observed exact layout among its Ten-Win Runs;
ties use canonical layout ordering.

`p75_ten_win_final_day` uses the nearest-rank method over known Ten-Win final
days:

```text
sorted_days[ceil(0.75 * count) - 1]
```

It is `null` only when no Ten-Win Run has a known final day.

### Build score and selection

The score is the 95% Wilson lower bound of the observed Ten-Win rate, scaled
to an integer:

```text
n = completed_run_count
k = ten_win_run_count
p = k / n
z = 1.96

lower = (p + z²/(2n) - z*sqrt(p*(1-p)/n + z²/(4n²)))
        / (1 + z²/n)

score = round(lower * 1,000,000)
```

This one score accounts for both observed rate and evidence volume. Rank,
speed, losses, and card tier do not add separate score weights.

For each hero:

1. sort candidates by descending `score`, descending `ten_win_run_count`,
   ascending `p75_ten_win_final_day` with `null` last, then Build Identity;
2. keep the first 500 candidates;
3. for every candidate card not represented in those 500, append the
   highest-ranked candidate containing that card.

The third step preserves Mod recall for rare selected cards without changing
the quality order of the core corpus.

## Publication

The two products are independent. Each is validated against its own schema
and consumer parser before its R2 object is replaced. Failure to calculate or
validate one product leaves that product's existing object unchanged and does
not prevent a valid independent product from being replaced.

Object replacement is atomic from the consumer's perspective. Both keys use:

```text
Cache-Control: public,max-age=60,must-revalidate
Content-Type: application/json
```

Consumers use `window.end` as data freshness. They do not infer completeness
from wall-clock time.

## Run report

Every invocation emits one structured report to local status and logs. This
report is operational evidence and is not a public R2 object.

```json
{
  "window": {
    "start": "2026-08-05",
    "end": "2026-08-11",
    "days": 7
  },
  "downloads": {
    "expected_bundles": 168000,
    "succeeded_bundles": 167996,
    "failed_bundles": 4
  },
  "facts": {
    "raw_runs": 240000,
    "discarded_unknown_hero": 27,
    "discarded_unknown_final_rank": 7300,
    "included_runs": 232673,
    "included_battles": 2584031
  },
  "heroes": {
    "participating_runs": 232673,
    "participating_matchup_battles": 2584031,
    "published": false
  },
  "builds": {
    "eligible_layout_runs": 184321,
    "candidate_builds": 12680,
    "published_builds": 4127,
    "published": false
  }
}
```

`failed_bundles > 0` prevents the affected Source Hour from completing. The
report may therefore describe attempted work while both public objects remain
unchanged. A successful report records zero failed Bundles and the actual
participation counts used by each product.

## Contract invariants

Before publication, the analyzer verifies at least:

- exactly seven distinct consecutive days matching `window`;
- exactly one row for each canonical hero and stored segment per day;
- row segments are only `legend` or `non_legend`;
- all counts are non-negative integers;
- `ten_win = perfect + gold`;
- outcome counts do not exceed `scored`, and `scored <= completed`;
- `known_count <= ten_win`;
- every Matchup satisfies `decided = wins + losses`;
- no `battle_days` field exists;
- every Build references valid card, enchantment, and Build indices;
- every Build layout occupies exactly 10 slots;
- emitted Builds and `card_index` agree in both directions;
- the Mod's schema-2 parser accepts `builds/latest.json`.
