#!/bin/bash
# Project restructuring script
# This script reorganizes the MF_ETL project into a proper structure

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR"

echo "MF_ETL Project Restructuring Script"
echo "===================================="
echo ""
echo "This script will restructure your project to follow best practices."
echo "All existing code will be preserved."
echo ""
read -p "Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

# Create new directory structure
echo "Creating directory structure..."

mkdir -p src/mf_etl/core
mkdir -p src/mf_etl/fetchers
mkdir -p src/mf_etl/validators
mkdir -p src/mf_etl/services
mkdir -p src/mf_etl/utils

mkdir -p services/api/routes
mkdir -p services/api/models
mkdir -p services/api/middleware
mkdir -p services/enrichment

mkdir -p demos
mkdir -p tests/unit
mkdir -p tests/integration
mkdir -p tests/fixtures

mkdir -p docs
mkdir -p scripts
mkdir -p notebooks

mkdir -p config
mkdir -p .github/workflows

echo "✓ Directory structure created"

# Copy files to new locations
echo "Moving files to new structure..."

# Move fetchers
cp -v src/fetchers/* src/mf_etl/fetchers/ 2>/dev/null || true
cp -v src/utils/fund_resolver.py src/mf_etl/services/ 2>/dev/null || true

# Move validators
cp -v src/validators/* src/mf_etl/validators/ 2>/dev/null || true

# Move utils
cp -v src/utils/config_loader.py src/mf_etl/utils/ 2>/dev/null || true
cp -v src/utils/logger.py src/mf_etl/utils/ 2>/dev/null || true

# Move API files
cp -v etl_service/main.py services/api/ 2>/dev/null || true
cp -v etl_service/models/* services/api/models/ 2>/dev/null || true
cp -v etl_service/validators/holding_validator.py services/ 2>/dev/null || true

# Move enrichment
cp -v etl_service/enrichment/* services/enrichment/ 2>/dev/null || true

# Move demos
cp -v demo.py demos/end_to_end_demo.py 2>/dev/null || true
cp -v test_api.py demos/ 2>/dev/null || true
cp -v test_api.ps1 demos/ 2>/dev/null || true

# Move docs
cp -v FUND_RESOLUTION_FLOW.md docs/ 2>/dev/null || true
cp -v PROJECT_STRUCTURE.md docs/ 2>/dev/null || true

echo "✓ Files moved to new structure"

# Create __init__ files
echo "Creating Python package files..."

for dir in src src/mf_etl src/mf_etl/core src/mf_etl/fetchers src/mf_etl/validators src/mf_etl/services src/mf_etl/utils \
           services services/api services/api/routes services/api/models services/api/middleware services/enrichment \
           demos tests tests/unit tests/integration tests/fixtures notebooks; do
    touch "$dir/__init__.py"
done

echo "✓ Python package files created"

echo ""
echo "===================================="
echo "Restructuring Summary"
echo "===================================="
echo ""
echo "New structure created:"
echo "  src/mf_etl/          - Core business logic"
echo "  services/api/        - FastAPI application"
echo "  services/enrichment/ - Enrichment service"
echo "  demos/               - Demo scripts"
echo "  tests/               - Test suite"
echo "  docs/                - Documentation"
echo "  scripts/             - Utility scripts"
echo ""
echo "Next steps:"
echo "  1. Review imports in all Python files"
echo "  2. Update requirements.txt with new package structure"
echo "  3. Create setup.py and pyproject.toml"
echo "  4. Update tests with proper fixtures"
echo "  5. Update documentation references"
echo "  6. Remove old directories after verification"
echo ""
echo "Run this to check the new structure:"
echo "  tree -L 3 src services demos tests docs"
echo ""
