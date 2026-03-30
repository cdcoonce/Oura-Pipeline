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

Most skills are provided by the `data-pipeline@claude-workflow` plugin. Local skills in `.claude/skills/` are project-specific additions only.

### Local Skills

#### `/deploy`

**Trigger when:** user asks to deploy, redeploy, or push to AWS Lambda.

#### `/github-cli`

**Trigger when:** user needs to interact with GitHub — issues, pull requests, PR reviews, CI/CD pipelines, or pushing changes.

#### `/git-guardrails-claude-code`

**Trigger when:** user wants to prevent destructive git operations, add git safety hooks, or block git push/reset in Claude Code.

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
