> [!WARNING]
> **本仓库已归档（只读）。** BazaarPlusPlus 的开发已迁移至 monorepo：
> **[BazaarPlusPlus/BazaarPlusPlus](https://github.com/BazaarPlusPlus/BazaarPlusPlus)** → [`bazaarplusplus-analyzer/`](https://github.com/BazaarPlusPlus/BazaarPlusPlus/tree/master/bazaarplusplus-analyzer)
>
> 这里只保留迁移前的提交历史，供查阅。Issue / PR 请提交到主仓库；下载安装请前往 [bazaarplusplus.com/download](https://bazaarplusplus.com/download)。以下内容为归档时的快照，可能已过时。
>
> **Archived (read-only).** Development moved to the monorepo linked above; this repository is kept for its commit history only. Please file issues and pull requests there. The content below is a snapshot and may be outdated.

# BazaarPlusPlus Analyzer

The analyzer converts verified Bundle V5 deliveries into local hourly facts and
two independent current snapshots:

```text
analyzer-v5/heroes/latest.json
analyzer-v5/builds/latest.json
```

## Operate

Install the locked environment with `uv sync --locked`, copy `.env.example` to `.env`,
and fill in the required values. Configuration comes only from the repository
`.env`.

```bash
uv run bpp run
uv run bpp run --no-publish
uv run bpp status --json
uv run bpp verify --deep
```

Use `uv run bpp --help` and `uv run bpp <command> --help` for the complete
operator interface.

## Documentation

- Domain terms and boundaries: `CONTEXT.md`
- Pipeline recovery and publication invariants: `docs/architecture.md`
- Consumer calculations and cross-field semantics: `docs/specs/consumer-data-contract.md`
- JSON wire shapes: `contracts/v5/*.schema.json`
- Measured operational limits: `docs/measurements.md`

## Quality gate

```bash
uv run ruff format --check .
uv run ruff check .
uv run ty check
uv run pytest
```
