# Contributing

This is a template repository: contributions here should improve the
reference itself (patterns, docs, fixes), not add product features specific
to any one service. If you've forked this to build a real service, these
conventions are a reasonable starting point but yours to change.

## Getting set up

```bash
poetry install       # install dependencies
make up               # docker compose up --build -d; runs migrations, starts the API
make demo             # POST an order, wait for the consumer, GET it back
```

See [README.md](README.md) for the full quickstart and
[CLAUDE.md](CLAUDE.md) for the architecture reference.

## Before you open a PR

```bash
make fmt        # poetry run black . && poetry run ruff check --fix .
make lint       # poetry run ruff check .
make test       # unit tests, no Docker required
make test-all   # full suite, including testcontainers integration tests
```

`make test-all` is required, not optional, before opening a PR — see
["Why testing locally with Docker matters"](BEST_PRACTICES.md#4-testing-strategy-unit-vs-integration)
in `BEST_PRACTICES.md`. The unit suite mocks Postgres and Redis; only the
full suite (or `make up && make demo`) proves the change works against the
real dependencies.

## Code style

- Formatting: [Black](https://black.readthedocs.io/), line length 88.
- Linting: [Ruff](https://docs.astral.sh/ruff/), selecting `E`, `F`, `W`,
  `B`, `I` (with `E501` ignored — Black already enforces line length).
- Import order: standard library, then third-party, then local — Ruff's `I`
  rule enforces this; `make fmt` fixes it automatically.
- No comments explaining *what* code does — names should already make that
  clear. A comment is only worth adding for a non-obvious *why* (a hidden
  constraint, a workaround, something that would surprise a reader).

## Database changes

Schema changes go through Alembic exclusively — there is no `create_all`
anywhere in this codebase.

```bash
make revision m="describe the change"   # alembic revision --autogenerate
make migrate                             # alembic upgrade head
```

Every revision must implement a working `downgrade()`.
`tests/integration/test_migrations.py` exercises
`upgrade head → downgrade base → upgrade head` and will fail if it doesn't.

## Adding a new event type

1. Add the payload model under `messaging/models/events/`.
2. Add the event type to the `Literal` and to the `EventPayload` union in
   `messaging/models/envelope.py`.
3. Write a handler under `messaging/consumer/handlers/`. Handlers must be
   idempotent — Redis Streams delivery is at-least-once, and redelivery is
   expected, not a bug.
4. Register the handler in `HANDLERS` in
   `messaging/consumer/dispatcher.py`.

## Testing expectations

- New logic in `core/services/`, `messaging/consumer/`, or `db/repositories/`
  needs a unit test in `tests/unit/`, with Postgres/Redis mocked.
- A change to the request/response flow, the consumer's failure handling, or
  a migration needs an integration test in `tests/integration/` (real
  Postgres and Redis via testcontainers).
- Don't rely on the unit suite alone to validate a change that touches the
  database or Redis — see `make test-all` above.

## Commit messages

Keep commits scoped to one logical change, and write the message around the
*why*, not a restatement of the diff. Match the existing history's style
(`git log --oneline`), e.g. `fix: ...`, `docs: ...`, `test: ...`,
`style: ...`.

## Documentation

If a change affects a documented behavior or pattern, update the relevant
doc in the same PR:

- `README.md` — quickstart, demo flow, "make it yours" checklist.
- `CLAUDE.md` — architecture reference used for AI-assisted development.
- `BEST_PRACTICES.md` — the practices behind the structure, and why they
  exist.
