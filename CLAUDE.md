# Development Standards

This file is auto-loaded every conversation. It defines how Claude should work in this project.

## Methodology

### TDD — Test-Driven Development

Write the test first. Watch it fail. Write minimal code to pass. No production code without a failing test.
Full process: [.claude/docs/tdd.md](.claude/docs/tdd.md)

### Root Cause Tracing

Never fix at the symptom. Trace backward through the call chain to the original trigger, then fix at the source.
Full process: [.claude/docs/root-cause-tracing.md](.claude/docs/root-cause-tracing.md)

### Subagent-Driven Development

When executing a plan with multiple independent tasks, dispatch a fresh subagent per task with code review between each.
Full process: [.claude/docs/subagent-development.md](.claude/docs/subagent-development.md)

### Parallel Agent Dispatch

When 3+ unrelated failures need investigation, dispatch one agent per independent problem domain concurrently.
Full process: [.claude/docs/parallel-agents.md](.claude/docs/parallel-agents.md)

## Planning

Write implementation plans to `docs/plans/{file_name}.md` before starting non-trivial work. Once a plan has been fully implemented, move it to `docs/archive/`.

## Code Style

- Descriptive variable names (`private_key_bytes` not `pkb`)
- SOLID, DRY, YAGNI — simplicity over complexity
- Type hints on all function signatures
- Numpy-style docstrings for public functions

## Skills

Skills live in `.claude/skills/`. Each `SKILL.md` defines an invocable skill with trigger conditions.

### `/daa-code-review`

**Trigger when:** user asks for a "code review", "quality check", pre-commit review, or wants code analyzed for issues.
**Output:** Save markdown report to `docs/code_reviews/{YYYY-MM-DD}_{file_name}.md`.

### `/github-cli`

**Trigger when:** user needs to interact with GitHub — issues, pull requests, PR reviews, CI/CD pipelines, or pushing changes.

### `/commit`

**Trigger when:** user asks to commit, make a commit, save work, or when Claude needs to commit changes after completing a task.

### `/readme-generator`

**Trigger when:** user asks to create, generate, update, or improve a README, or says "document this project".
**References:** [.claude/skills/readme-generator/references/](.claude/skills/readme-generator/references/) — analysis methodology, mermaid guidelines, badge reference.

### `/grill-me`

**Trigger when:** user wants to stress-test a plan, get grilled on their design, or mentions "grill me".

### `/plan-ceo-review`

**Trigger when:** user asks for a plan review, CEO review, mega review, or wants a plan challenged/stress-tested before implementation.
**References:** [.claude/skills/plan-ceo-review/references/](.claude/skills/plan-ceo-review/references/) — review sections, required outputs.

### `/project-context`

**Trigger when:** user asks to create, update, or refresh project context, says "update project.md", or when onboarding Claude to a new repo.
**Output:** `.claude/docs/project.md`

### `/tdd`

**Trigger when:** user wants to build features or fix bugs using TDD, mentions "red-green-refactor", wants integration tests, or asks for test-first development.

### `/triage-issue`

**Trigger when:** user reports a bug, wants to file an issue, mentions "triage", or wants to investigate and plan a fix for a problem.

### `/write-a-prd`

**Trigger when:** user wants to write a PRD, create a product requirements document, or plan a new feature.

### `/prd-to-plan`

**Trigger when:** user wants to break down a PRD, create an implementation plan, plan phases from a PRD, or mentions "tracer bullets".

### `/prd-to-issues`

**Trigger when:** user wants to convert a PRD to issues, create implementation tickets, or break down a PRD into work items.

### `/dev-cycle`

**Trigger when:** user says "dev cycle", "development workflow", "full development pipeline", or wants the full end-to-end lifecycle from brainstorm through PR. **Disambiguation:** if user only wants a PRD, route to `/write-a-prd`; only a plan, route to `/prd-to-plan`.
**References:** [.claude/skills/dev-cycle/references/](.claude/skills/dev-cycle/references/) — phase transitions, state file schema.

### `/request-refactor-plan`

**Trigger when:** user wants to plan a refactor, create a refactoring RFC, or break a refactor into safe incremental steps.

### `/improve-codebase-architecture`

**Trigger when:** user wants to improve architecture, find refactoring opportunities, consolidate tightly-coupled modules, or make a codebase more testable.

### `/design-an-interface`

**Trigger when:** user wants to design an API, explore interface options, compare module shapes, or mentions "design it twice".

### `/git-guardrails-claude-code`

**Trigger when:** user wants to prevent destructive git operations, add git safety hooks, or block git push/reset in Claude Code.

### `/write-a-skill`

**Trigger when:** user wants to create, write, or build a new Claude Code skill.

## Project Context

See [.claude/docs/project.md](.claude/docs/project.md) for project-specific details (tech stack, architecture, test markers).

## Testing

- Run tests: `uv run pytest`
- Run with coverage: `uv run pytest --cov=src --cov-report=term-missing`
- Prefer real code over mocks
- Test fixtures in `tests/fixtures/`
- For SQL transformations, test with sample data in fixtures

## Data Pipeline Conventions

- SQL files use lowercase keywords
- Pipeline stages should be idempotent where possible
- Log row counts at each transformation stage
