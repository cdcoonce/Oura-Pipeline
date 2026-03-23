---
name: dev-cycle
description: >
  Orchestrate the full GitHub-issues-driven development lifecycle.
  7-phase pipeline from brainstorm through PR with state tracking
  and cross-conversation resume. Use when user says "dev cycle",
  "development workflow", "full development pipeline", or invokes /dev-cycle.
---

# Dev Cycle Orchestrator

Orchestrate the full development lifecycle: brainstorm → plan → review → issues → implement → code review → PR.

**Disambiguation:** If the user only wants a PRD, route to `/write-a-prd`. If they only want a plan, route to `/prd-to-plan`. This skill is for the full end-to-end lifecycle.

## The 7-Phase Pipeline

Every phase is mandatory. No phase can be skipped.

| #   | Phase           | Delegates To                                           | Gate Condition                    |
| --- | --------------- | ------------------------------------------------------ | --------------------------------- |
| 1   | **Brainstorm**  | `write-a-prd`                                          | Issue URL recorded                |
| 2   | **Plan**        | `prd-to-plan`                                          | Plan file exists at `docs/plans/` |
| 3   | **CEO Review**  | `plan-ceo-review` (recommend HOLD SCOPE)               | Review complete, user approves    |
| 4   | **Issues**      | Orchestrator (plan slices → GitHub issues)             | All issue URLs recorded           |
| 5   | **Implement**   | Orchestrator (`tdd` per issue, `subagent-development`) | All issues resolved, tests pass   |
| 6   | **Code Review** | `daa-code-review`                                      | Clean review                      |
| 7   | **PR**          | `commit` + `github-cli`                                | PR URL recorded                   |

## Re-entry Logic

On every invocation:

### No arguments (`/dev-cycle`)

1. Scan `docs/dev-cycle/` for `*.state.md` files with `status: in_progress`
2. If one → ask: "Resume **{feature}** (`{branch}`)? Currently at **{phase}**."
3. If multiple → list with branch names, ask which to resume
4. If none → ask: "What feature are you working on?" → start Phase 1

### With argument (`/dev-cycle {slug}`)

1. Look for `docs/dev-cycle/{slug}.state.md`
2. Found → resume at `current_phase`
3. Not found → create state file, start Phase 1

### Slug Collision

When creating a new state file, check `docs/dev-cycle/` for existing slugs. If the slug already exists (abandoned or completed), suffix with `-2`, `-3`, etc.

### Context Loading on Resume

Before continuing, load ALL referenced artifacts:

- **Brainstorm:** `gh issue view` the PRD issue
- **Plan:** Read plan file from disk
- **CEO Review:** Read the plan file (includes review revisions)
- **Issues:** `gh issue view` each implementation issue
- **Implement:** Check git status on feature branch, review closed issues

Present summary: "Resuming **{feature}** at **{phase}**. Here's where we left off: ..."

## Phase Execution

### Phase 1: Brainstorm

Invoke `write-a-prd`. Trust the skill's internal flow (interview → PRD → GitHub issue). Record the issue URL in the state file.

### Phase 2: Plan

Pass the PRD issue URL to `prd-to-plan`. Record the plan file path (at `docs/plans/{feature}.md`).

### Phase 3: CEO Review

Pass plan file path to `plan-ceo-review`. Recommend HOLD SCOPE mode but let the skill's own mode selection (Step 0F) run. Record when review is complete and user approves.

### Phase 4: Issues

**Owned by the orchestrator.** Read the plan's vertical slices. For each slice, create a GitHub issue using `gh issue create` that:

- References the PRD issue
- Includes acceptance criteria from the plan
- Is created in dependency order

Record each issue URL in the Issues table immediately after creation. See [references/phase-transitions.md](references/phase-transitions.md) for partial-completion recovery.

### Phase 5: Implement

**Owned by the orchestrator.** Create feature branch `feat/{feature-slug}` (see branch handling in [references/phase-transitions.md](references/phase-transitions.md)).

Dispatch one subagent per GitHub issue following `subagent-development` methodology:

- Each subagent invokes the `tdd` skill
- Code review runs between each subagent dispatch
- State file is updated after each subagent completes (not batched)

Log per-subagent events:

- `"Subagent started for issue #N: {title}"`
- `"Subagent completed for issue #N: {pass/fail}"`
- `"Code review after issue #N: {clean/blocking issues found}"`

### Phase 6: Code Review

Invoke `daa-code-review` against all changed files on the feature branch. If blocking issues found → fix, re-run. Loop until clean.

If architectural issues requiring plan rework → trigger backwards transition to Phase 2.

### Phase 7: PR

Check for conflicts with default branch first. Invoke `commit` for conventional commit, then `github-cli` to open PR. Record PR URL, set `status: completed`. Then run the archival step (see Archival below).

## State File

Each feature tracked at `docs/dev-cycle/{feature-slug}.state.md`. See [references/state-file-schema.md](references/state-file-schema.md) for full format, field definitions, and transition rules.

## Failure & Recovery

See [references/phase-transitions.md](references/phase-transitions.md) for:

- Phase retry logic (blocked → retry on next invocation)
- Backwards transitions (implement/code_review → plan)
- Feature abandonment. Archived files are moved to `docs/archive/`.

## Branch Management

- **Phases 1–4:** Run on current branch (documentation only)
- **Phase 5:** Creates `feat/{feature-slug}` branch
- **Phase 7:** PRs to the default branch (detected via `gh repo view --json defaultBranchRef`)
- **Resume at Phase 5+:** Check out feature branch if not already on it

## Archival

When a feature reaches a terminal state (`completed` or `abandoned`), archive its artifacts:

1. Create archive directories: `mkdir -p docs/archive/dev-cycle docs/archive/plans`
2. Move the state file: `git mv docs/dev-cycle/{slug}.state.md docs/archive/dev-cycle/`
3. Move the plan file: read the plan path from the artifacts table, then `git mv {plan_path} docs/archive/plans/`
4. Commit the moves with message: `chore(dev-cycle): archive {slug}`

Archival runs automatically:

- At the end of Phase 7 (after PR URL is recorded and status is set to `completed`)
- On feature abandonment (after status is set to `abandoned`)
