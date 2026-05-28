# Lakeforge Customer Quickstart — Lakehouse Buildout (Claude Code shown; any supported agent works)

LakeForge is an open‑source multi‑agent system that automatically generates Databricks Lakehouse pipelines from user specifications. It leverages intelligent orchestration to design, optimize, and implement pipelines following industry best practices — making it easier to go from idea to production‑ready lakehouse with minimal effort.
Install Lakeforge, initialize a project with the `**lakehouse-buildout**` package, and run your first feature with Claude Code — or any other agent supported by the `lakehouse-buildout` package. One page; everything you need to get started.

## Prerequisites


| Tool                                  | Version           | Why                                                                                                                                                                    |
| ------------------------------------- | ----------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Python                                | ≥ 3.11            | Lakeforge runtime                                                                                                                                                      |
| git                                   | any recent        | Worktrees per work package                                                                                                                                             |
| AI agent CLI (`claude`, `gemini`, `cursor`, `codex`, or `copilot`) | recent, authed    | Drives the slash commands. Claude is the running example below; substitute your agent's CLI in the install/init/launch steps.                                          |
| `uv`                                  | on `PATH`         | Required by `ai-dev-kit` (auto-installed). Install: [https://docs.astral.sh/uv/getting-started/installation/](https://docs.astral.sh/uv/getting-started/installation/) |
| Databricks workspace                  | reachable, authed | Where the lakehouse is built                                                                                                                                           |


### Pick your agent

The `lakehouse-buildout` package requires an agent supported by `ai-dev-kit`. Currently supported: **`claude`**, **`codex`**, **`copilot`**, **`cursor`**, **`gemini`**. After installing the wheel (next section), you can also confirm the live list by running `lakeforge init --help` and reading the `--ai` choices.

The rest of this guide uses `claude` as the running example — substitute your agent's name in `--ai <agent>` and your agent's CLI binary in the `claude` launch step (§3).

---

## 1. Install — or upgrade

**First-time install** (the wheel you were sent):  
  
The wheel file can be also found here -> [https://github.com/jincejames/LakeForge](https://github.com/jincejames/LakeForge)

```bash
python3 -m pip install --user ./lakeforge_cli-<version>-py3-none-any.whl
# Add the user-bin dir to PATH if it isn't already:
USER_BIN="$(python3 -c 'import sysconfig; print(sysconfig.get_path(\"scripts\", scheme=\"posix_user\"))')"
echo "export PATH=\"$USER_BIN:\$PATH\"" >> ~/.zshrc && source ~/.zshrc
lakeforge --version
```

If you use `pyenv` or switch Python versions later, reinstall the wheel against the now-active interpreter (`python3 -m pip install --user --force-reinstall ./lakeforge_cli-<version>-py3-none-any.whl`).

**Upgrade an existing install** (new wheel, same project):

```bash
python3 -m pip install --user --upgrade ./lakeforge_cli-<new-version>-py3-none-any.whl
cd /path/to/your/lakehouse-project
lakeforge upgrade
```

`lakeforge upgrade` reads `.lakeforge/config.yaml`, so the lakehouse-buildout mission and Claude selection are preserved — no need to re-run `init`.

---

## 2. Initialize the project

```bash
mkdir my-lakehouse && cd my-lakehouse
git init
lakeforge init . --ai claude --package lakehouse-buildout --force
git add . && git commit -m "Initial Lakeforge lakehouse project"
```

This selects your chosen agent (`claude` in the example — swap to `codex`, `copilot`, `cursor`, or `gemini` as needed) as the only agent, locks in the `databricks-lakehouse-buildout` mission, and auto-installs `ai-dev-kit` (Databricks skills + MCP tools for SQL, pipelines, Unity Catalog). What it scaffolds:

- `.lakeforge/` — project config (`config.yaml`) and mission templates
- `.claude/commands/` — the `/lakeforge.*` slash commands Claude reads
- `.claude/skills/` + `.claude/mcp.json` — `ai-dev-kit` Databricks skills and the MCP server registration (50+ Databricks tools: SQL, jobs, pipelines, Unity Catalog)
- `lakeforge-specs/` — where feature specs, plans, and tasks live
- `.lakeforge/memory/constitution.md` — created the first time you run `/constitution` (see §5)



ps: Instead of claude, it can be any agent availble in your local

---

## 3. Open your AI agent in the project (Claude Code shown)

```bash
cd my-lakehouse
claude
```

Replace `claude` with your agent's CLI binary (`gemini`, `cursor`, `codex`, etc.) — the slash commands work identically across agents.

Type `/` inside your agent's CLI and you'll see the `/lakeforge.*` commands. Each agent uses its own normal auth (Claude: Anthropic API key / Bedrock / Vertex; Gemini: Google credentials; Cursor: its own login; etc.) — Lakeforge has no LLM credentials of its own.

---

## 4. Daily workflow — the commands

> Run **slash commands** inside your agent's CLI. Run `**lakeforge …`** commands in a terminal at the project root.


| Command                    | What it does                                                                                          | Why it's there                                                                                                            |
| -------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `/constitution`            | Captures lakehouse principles + customer non-negotiables. Writes `.lakeforge/memory/constitution.md`. | Anchors every later decision (bronze-first, layer separation, compliance) so the agent doesn't drift. **Run this first.** |
| `/specify <idea>`          | Generates `lakeforge-specs/###-feature/spec.md` from a natural-language idea.                         | Forces clarity on outcome + acceptance criteria before any code is written.                                               |
| `/plan`                    | Produces `plan.md` — tech approach, medallion-layer impacts, risks.                                   | Separates "what" from "how" so reviewers can challenge the approach early.                                                |
| `/tasks`                   | Breaks the plan into Work Packages (WP01, WP02, …) with dependencies.                                 | Each WP is an atomic unit; enables parallel implementation.                                                               |
| `lakeforge implement WP01` | (Terminal.) Creates a worktree + branch for that WP and moves the kanban lane to `doing`.             | Isolates WPs so multiple agents can work in parallel. Use `--base WP##` for chained dependents.                           |
| `/implement`               | (Inside the worktree's agent CLI.) Executes the WP's tasks against Databricks (SQL, pipelines, UC).   | The actual build; uses `ai-dev-kit` MCP tools for live verification.                                                      |
| `/review`                  | Reviews the WP, requests changes, or advances it to `for_review`.                                     | Quality gate before acceptance; can bounce a WP back to `doing`.                                                          |
| `/accept`                  | Verifies acceptance criteria, marks the WP `done`.                                                    | Closes the loop on the spec's success conditions.                                                                         |
| `/merge`                   | Merges all done WPs of the feature into `main` and cleans up worktrees.                               | One command consolidates the feature; preflight checks prevent dirty merges.                                              |
| `lakeforge dashboard`      | (Terminal.) Opens a local kanban board.                                                               | Visibility into which WPs are `planned` / `doing` / `for_review` / `done`.                                                |


For every flag and option, run `lakeforge --help` or `lakeforge <command> --help` in a terminal.

---

## 5. Add customer context to the constitution

The constitution (`.lakeforge/memory/constitution.md`) is auto-consulted by `/plan`, `/specify`, and every other command — so customer-specific facts dropped here propagate everywhere automatically. `/constitution` is **idempotent**: re-run it any time the customer's policies change; it preserves non-conflicting principles and bumps the version footer.

> Run `/constitution` **before** `/specify` so the first spec inherits the context.

### Create with customer context

Pass the context inline as the argument to `/constitution`:

```
/constitution Project: Acme Retail Lakehouse.
Catalog naming: <env>_acme_<layer> (dev_acme_bronze, prod_acme_gold).
Compliance: GDPR — PII columns must be tokenized at Bronze, max 90-day retention.
Data owners: ingestion@acme.com (bronze), analytics@acme.com (silver/gold).
Tag every Unity Catalog object with: cost_center, data_owner, compliance_level.
```

The agent merges this into the standard 8 Lakehouse Principles and the Databricks Well-Architected pillars, then writes the file.

### Modify later

Just re-run `/constitution` with the new facts. Version bumps follow:

- **PATCH** — naming refinement, doc-only update
- **MINOR** — new source, new governance rule, new quality gate
- **MAJOR** — layer-architecture or breaking data-model change

Example amendment:

```
/constitution Add: SOX controls for Gold layer — financial reconciliation tables require dual approval before deploy. Owner: finance-data@acme.com. New tag: sox_scope=true on all Gold reconciliation tables.
```

### Where customer-specific facts land

When `/constitution` writes the file, it generates these sections — they are the natural homes for customer context:


| Section heading                           | Use it for                                                            |
| ----------------------------------------- | --------------------------------------------------------------------- |
| `## Technical Constraints`                | Catalog/schema naming, environment URLs, account IDs                  |
| `## Risk Escalation Triggers`             | Compliance thresholds (GDPR / HIPAA / SOX), data-source size cutoffs  |
| `## Governance`                           | Approval workflows, amendment process, source-qualification criteria  |
| `### Principle VIII: Resource Provenance` | Custom UC tags (e.g. `cost_center`, `compliance_level`, `data_owner`) |


### Manual edit fallback

`.lakeforge/memory/constitution.md` is plain markdown — direct edits are fine. If you do, bump the footer yourself:

```
**Version**: 1.2.0 | **Ratified**: 2026-01-15 | **Last Amended**: 2026-05-28
```

---

## 6. Verify

```bash
lakeforge verify       # sanity-check .lakeforge/, agent dirs, git state
lakeforge dashboard    # local kanban; no network needed
```

