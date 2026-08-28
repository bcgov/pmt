.PHONY: up down logs test test-all lint fmt demo health

up:                ## Start Redis and the worker
	docker compose up --build -d
	@echo "Worker health on http://localhost:8000/health"

down:
	docker compose down

logs:
	docker compose logs -f worker

health:
	@curl -s -i http://localhost:8000/health

test:              ## Unit tests only — no Docker required
	poetry run pytest -m "not integration" --junitxml=reports/junit.xml --cov=. --cov-report=xml:reports/coverage.xml --cov-report=html:reports/htmlcov

test-all:          ## Everything, including testcontainers
	poetry run pytest --junitxml=reports/junit.xml --cov=. --cov-report=xml:reports/coverage.xml --cov-report=html:reports/htmlcov

lint:
	poetry run ruff check .

fmt:
	poetry run black . && poetry run ruff check --fix .

demo:              ## Publish one order twice; watch it confirm exactly once
	@echo "--- publishing OrderCreated x2 (same ref)"
	@docker compose exec worker python -m cli publish \
		--ref demo-1 --item widget --quantity 3 --unit-price-cents 450 --count 2
	@echo "--- waiting for the consumer..."
	@sleep 3
	@echo "--- state in Redis"
	@docker compose exec redis redis-cli -a redis --no-auth-warning -n 1 HGETALL order:demo-1
	@echo "--- worker log (one confirm, one already-processed, one confirmation received)"
	@docker compose logs --tail=30 worker
