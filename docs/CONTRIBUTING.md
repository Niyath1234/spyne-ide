# Contributing to Spyne IDE

Thank you for your interest in contributing to Spyne IDE! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Python 3.10+
- Rust 1.70+
- Git
- Docker (optional, for containerized development)

### Setup Steps

1. **Fork and Clone**
   ```bash
   git clone https://github.com/your-username/spyne-ide.git
   cd spyne-ide
   ```

2. **Create Virtual Environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # If exists
   ```

4. **Setup Environment**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

5. **Run Tests**
   ```bash
   pytest tests/ -v
   ```

## Code Style

### Python

- Follow PEP 8 style guide
- Use type hints for function signatures
- Maximum line length: 100 characters
- Use `black` for formatting (if configured)
- Use `flake8` for linting

### Rust

- Follow rustfmt defaults
- Use `cargo clippy` for linting
- Document public APIs with doc comments

## Development Workflow

1. **Create a Branch**
   ```bash
   git checkout -b feature/your-feature-name
   # or
   git checkout -b fix/your-bug-fix
   ```

2. **Make Changes**
   - Write clean, documented code
   - Add tests for new features
   - Update documentation as needed

3. **Run Tests**
   ```bash
   pytest tests/ -v
   cargo test  # For Rust code
   ```

4. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: Add new feature"
   # Use conventional commits:
   # feat: New feature
   # fix: Bug fix
   # docs: Documentation changes
   # test: Test additions/changes
   # refactor: Code refactoring
   # chore: Maintenance tasks
   ```

5. **Push and Create PR**
   ```bash
   git push origin feature/your-feature-name
   # Then create a Pull Request on GitHub
   ```

## Testing Guidelines

- Write tests for all new features
- Maintain or improve test coverage
- Include both unit and integration tests
- Test error cases and edge cases

## Documentation

- Update README.md for user-facing changes
- Add docstrings to new functions/classes
- Update API documentation if endpoints change
- Keep architecture docs up to date

## Pull Request Process

1. **Ensure Tests Pass**
   - All tests must pass
   - Code must be linted
   - No new warnings

2. **Update Documentation**
   - Update README if needed
   - Add/update docstrings
   - Update API docs

3. **Create PR**
   - Use clear, descriptive title
   - Describe changes in detail
   - Link related issues
   - Request review from maintainers

4. **Address Feedback**
   - Respond to review comments
   - Make requested changes
   - Keep PR updated with main branch

## Code Review Guidelines

- Be respectful and constructive
- Focus on code, not the person
- Suggest improvements, don't just point out issues
- Approve when satisfied

## Questions?

- Open an issue for questions
- Check existing documentation
- Ask in discussions

Thank you for contributing! 🎉

