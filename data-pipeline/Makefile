.PHONY: test test-unit test-integration test-coverage install-test-deps clean-test

# Install dependencies for the project
install-deps:
	pip install -r requirements.txt

# Install pre-commit hooks
install-pre-commit:
	pip install pre-commit
	pre-commit install

# Install test dependencies
install-test-deps:
	pip install -r tests/requirements.txt

# Run all tests
test: install-test-deps
	python -m pytest tests/ -v

# Run only unit tests
test-unit: install-test-deps
	python -m pytest tests/ -v -m "not integration"

# Run only integration tests
test-integration: install-test-deps
	python -m pytest tests/test_integration.py -v

# Run tests with coverage report
test-coverage: install-test-deps
	python -m pytest tests/ -v --cov=. --cov-report=html --cov-report=term-missing

# Run specific test file
test-file: install-test-deps
	python -m pytest tests/$(FILE) -v

# Run tests for specific service
test-service: install-test-deps
	python -m pytest tests/test_$(SERVICE).py -v

# Clean test artifacts
clean-test:
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# Run tests in watch mode (requires pytest-watch)
test-watch:
	pip install pytest-watch
	ptw tests/

# Generate test report
test-report: test-coverage
	@echo "Test coverage report generated in htmlcov/index.html"
	@echo "Open htmlcov/index.html in your browser to view the report"

# Lint test files
lint-tests:
	pip install flake8 black
	black tests/
	flake8 tests/ --max-line-length=88 --ignore=E203,W503

# Run all quality checks
quality-check: lint-tests test-coverage
	@echo "All quality checks completed"
