# ClearCare Compliance MVP Makefile

.PHONY: help up down sync-schemas test clean

help:
	@echo "Available targets:"
	@echo "  up            - Start all services with docker-compose"
	@echo "  down          - Stop all services"
	@echo "  sync-schemas  - Sync CMS JSON schemas from official repository"
	@echo "  test          - Run tests"
	@echo "  clean         - Clean up temporary files"

up:
	docker-compose up -d

down:
	docker-compose down

sync-schemas:
	@echo "Syncing CMS JSON schemas..."
	python scripts/sync_cms_schemas.py

test:
	@echo "Running tests..."
	python -m pytest tests/ -v

clean:
	@echo "Cleaning up..."
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	rm -rf .pytest_cache/
