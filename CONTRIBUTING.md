# RustDB Contributing Guide

Thank you for your interest in contributing to RustDB! We welcome any contributions to the project's development.

## 📋 Code of Conduct

By participating in this project, you agree to abide by our code of conduct. Please be respectful and constructive in communication.

## 🚀 How to Contribute

### 1. Bug Reports

If you found a bug:
1. Check if there's already an open issue with this problem
2. Create a new issue using the "Bug Report" template
3. Provide as much detail as possible for reproduction

### 2. Feature Proposals

To propose new features:
1. Create an issue with the "Feature Request" template
2. Describe the problem the feature solves
3. Suggest possible solutions

### 3. Code

#### Development Environment Setup

```bash
# Clone repository
git clone https://github.com/CrossEyedCat/RustDB.git
cd RustDB

# Install dependencies
cargo build

# Run tests
cargo test

# Code quality check
cargo clippy

# Format code
cargo fmt
```

#### Development Process

1. **Fork** the repository
2. Create a **feature branch** (`git checkout -b feature/amazing-feature`)
3. **Make changes** following coding standards
4. **Add tests** for new functionality
5. **Ensure** all tests pass (`cargo test`)
6. **Check code** with clippy (`cargo clippy`)
7. **Format code** (`cargo fmt`)
8. **Commit changes** (`git commit -am 'Add some amazing feature'`)
9. **Push branch** (`git push origin feature/amazing-feature`)
10. Create a **Pull Request**

## 📝 Coding Standards

### Rust

- Follow official Rust standards
- Use `rustfmt` for formatting (configuration in `rustfmt.toml`)
- Fix all `clippy` warnings (configuration in `.clippy.toml`)
- Cover code with tests
- Document public APIs

### Git

#### Commit Messages

Use Conventional Commits format:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: new feature
- `fix`: bug fix
- `docs`: documentation changes
- `style`: formatting, missing semicolons, etc.
- `refactor`: code refactoring
- `test`: adding tests
- `chore`: updating build tasks, configurations, etc.

**Examples:**
```
feat(storage): add B+ tree implementation
fix(parser): handle empty SQL statements
docs: update README with installation instructions
```

## 🧪 Testing

### Test Types

1. **Unit tests** - testing individual functions and methods
2. **Integration tests** - testing component interactions
3. **Benchmarks** - performance measurement

### Running Tests

```bash
# All tests
cargo test

# Specific test
cargo test test_name

# Tests with output
cargo test -- --nocapture

# Benchmarks
cargo bench
```

### Code Coverage

```bash
# Install cargo-llvm-cov
cargo install cargo-llvm-cov

# Generate coverage report
cargo llvm-cov --html
```

## 📚 Documentation

- Document all public APIs
- Include usage examples
- Update README.md when adding new features
- Use English for user documentation
- Use English for technical comments in code

## 🏗️ Architecture

Familiarize yourself with the documents:
- `ARCHITECTURE.md` - system architecture description
- `IMPLEMENTATION_CHECKLIST.md` - implementation plan
- `DEVELOPMENT.md` - development instructions

## 🔍 Code Review

All Pull Requests go through code review:

1. **Automated checks** must pass successfully
2. **At least one approve** from a maintainer
3. **All comments** must be resolved

### What We Check:

- Compliance with coding standards
- Presence and quality of tests
- Performance and security
- Compatibility with existing code
- Documentation quality

## 🚀 Releases

Releases are created automatically when tags are created:

```bash
git tag -a v0.2.0 -m "Release version 0.2.0"
git push origin v0.2.0
```

## 📞 Communication

- **Issues** - for bugs and feature proposals
- **Discussions** - for general questions and discussions
- **Pull Requests** - for proposing code changes

## 🏷️ Priorities

Current development priorities (see `IMPLEMENTATION_CHECKLIST.md`):

1. **Phase 1**: Basic infrastructure
2. **Phase 2**: Data storage
3. **Phase 3**: SQL parsing
4. **Phase 4**: Query execution
5. **Phase 5**: Optimization

## 🙏 Acknowledgments

We appreciate every contribution to the project, regardless of size. All contributors will be mentioned in the CONTRIBUTORS.md file.

---

**Thank you for contributing to RustDB! 🚀**
