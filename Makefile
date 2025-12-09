.PHONY: format lint test ci

format:
	black src tests
	ruff check --fix src tests

lint:
	ruff check src tests

test:
	pytest tests

ci: format lint test
