.PHONY: up down logs migrate revision test test-all lint fmt demo demo-s3

up:                ## Start Postgres, Redis and the API (runs migrations first)
	docker compose up --build -d
	@echo "API on http://localhost:8000/docs"

down:
	docker compose down

logs:
	docker compose logs -f api

migrate:           ## Apply migrations against the running database
	docker compose exec api alembic upgrade head

revision:          ## make revision m="add a column"
	docker compose exec api alembic revision --autogenerate -m "$(m)"

test:              ## Unit tests only — no Docker required
	poetry run pytest -m "not integration" --junitxml=reports/junit.xml --cov=. --cov-report=xml:reports/coverage.xml --cov-report=html:reports/htmlcov

test-all:          ## Everything, including testcontainers
	poetry run pytest --junitxml=reports/junit.xml --cov=. --cov-report=xml:reports/coverage.xml --cov-report=html:reports/htmlcov

lint:
	poetry run ruff check .

fmt:
	poetry run black . && poetry run ruff check --fix .

demo:              ## Watch one order go pending -> confirmed
	@echo "--- POST /orders"
	@curl -s -X POST http://localhost:8000/orders \
		-H 'Content-Type: application/json' \
		-d '{"order_ref":"demo-1","item":"widget","quantity":3}' | python3 -m json.tool
	@echo "--- waiting for the consumer..."
	@sleep 2
	@echo "--- GET /orders/demo-1"
	@curl -s http://localhost:8000/orders/demo-1 | python3 -m json.tool

demo-s3:           ## Watch an order get priced from S3 and land in the rollup
	@echo "--- price catalog in S3"
	@docker compose run --rm --entrypoint sh s3-put -c \
		"aws --endpoint-url http://seaweedfs:8333 s3 cp s3://pmt-bucket/config/prices.json -"
	@echo "--- POST /orders"
	@curl -s -X POST http://localhost:8000/orders \
		-H 'Content-Type: application/json' \
		-d '{"order_ref":"demo-s3-1","item":"widget","quantity":3}' | python3 -m json.tool
	@echo "--- waiting for the consumer..."
	@sleep 3
	@echo "--- GET /orders/demo-s3-1 (total_cents comes from S3)"
	@curl -s http://localhost:8000/orders/demo-s3-1 | python3 -m json.tool
	@echo "--- GET /rollups/$$(date -u +%F) (the object the consumer wrote)"
	@curl -s http://localhost:8000/rollups/$$(date -u +%F) | python3 -m json.tool
