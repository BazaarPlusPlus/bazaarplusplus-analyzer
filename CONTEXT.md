# BazaarPlusPlus Analyzer

BazaarPlusPlus Analyzer turns successfully delivered game runs into daily hero
metrics and a current build recommendation corpus. Its language distinguishes
Run outcomes from Battle outcomes and source completeness from consumer
freshness.

## Source collection

**Bundle**:
An immutable delivery item containing one Run and its associated Battles and
card snapshots.

**Source Hour**:
The UTC hour assigned from a Bundle's server-observed availability time.
_Avoid_: Upload hour, game hour

**Source Day**:
The UTC date composed of all 24 Source Hours for that date.
_Avoid_: Run day, game day

**Complete Source Day**:
A Source Day for which all 24 Source Hours are present and accepted.
_Avoid_: Latest day, successful day

**Source Epoch**:
An optional inclusive UTC date before which Source Days are outside the data
population. Pre-epoch days are not healed, sealed, or analyzed.

**Analysis Window**:
The sequence ending at the latest sealed Complete Source Day and extending
backward through consecutive Complete Source Days, with a minimum of one day
and a maximum of seven days. It never crosses the Source Epoch.
_Avoid_: Release window, lookback

## Runs and segments

**Accepted Run**:
A Run whose hero and final rank are recognized. A Run without a recognized
final rank, together with its associated Battles and card snapshots, is not
part of the analyzed population.
_Avoid_: Valid row, usable record

**Legend Segment**:
Accepted Runs whose canonical source value for final rank is `Legendary`.

**Non-Legend Segment**:
Accepted Runs whose final rank is recognized and is not Legend.

**All Segment**:
The union of the Legend Segment and Non-Legend Segment. It is derived by
addition rather than stored separately.

**Scored Run**:
A completed Accepted Run whose victory and loss values place it in a defined
outcome bucket.
_Avoid_: Completed Run

**Ten-Win Run**:
A completed Accepted Run with ten victories, including both perfect and
non-perfect results.

**Ten-Win Rate**:
Ten-Win Runs divided by completed Accepted Runs in the same hero, segment,
and time scope.
_Avoid_: Win rate, Battle win rate

## Battles and builds

**Matchup**:
The decided Battles played by one hero against another hero. A Matchup's
segment is owned by the player's Run, not by the opponent.
_Avoid_: Battle-day rate, hero win rate

**Matchup Win Rate**:
Player-side Battle wins divided by decided Matchup Battles.

**Build Identity**:
A hero plus the sorted multiset of card template identifiers in a completed
final board. Card position, tier, and enchantment are not part of the identity.
_Avoid_: Layout, board snapshot

**Representative Layout**:
The most frequently observed complete layout among a Build Identity's
Ten-Win Runs.

**Build Corpus**:
The current set of scored Build Identities and their representative layouts
used by the Mod for recommendation recall and ranking.
_Avoid_: Build release, build history
