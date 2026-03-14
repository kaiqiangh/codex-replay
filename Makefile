dev:
	pnpm dev

dev-web:
	pnpm dev:web

dev-api:
	pnpm dev:api

test:
	uv run --project services/api pytest -q
	pnpm --filter web test

test-api:
	uv run --project services/api pytest -q

test-web:
	pnpm --filter web test

typecheck:
	pnpm typecheck

build:
	pnpm build

smoke:
	bash scripts/ci/run-smoke.sh
