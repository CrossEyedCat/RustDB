# Руководство по CI/CD для RustBD

## 🚀 Обзор

Это руководство описывает настройку и использование системы непрерывной интеграции и развертывания (CI/CD) для проекта RustBD. Мы используем GitHub Actions для автоматизации процессов сборки, тестирования и развертывания.

## 🔧 Настройка GitHub Actions

### Основной workflow

```yaml
# .github/workflows/ci.yml
name: Continuous Integration

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

env:
  CARGO_TERM_COLOR: always
  RUST_BACKTRACE: 1

jobs:
  test:
    name: Test
    runs-on: ubuntu-latest
    strategy:
      matrix:
        rust: [stable, beta, nightly]
        target: [x86_64-unknown-linux-gnu, x86_64-apple-darwin, x86_64-pc-windows-msvc]
        exclude:
          - rust: nightly
            target: x86_64-apple-darwin
          - rust: nightly
            target: x86_64-pc-windows-msvc

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: ${{ matrix.rust }}
        target: ${{ matrix.target }}
        override: true

    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target
        key: ${{ runner.os }}-cargo-${{ matrix.rust }}-${{ matrix.target }}-${{ hashFiles('**/Cargo.lock') }}

    - name: Install target
      run: rustup target add ${{ matrix.target }}

    - name: Check formatting
      run: cargo fmt --all -- --check

    - name: Clippy check
      run: cargo clippy --all-targets --all-features -- -D warnings

    - name: Run tests
      run: cargo test --all-targets --all-features

    - name: Run tests with coverage
      if: matrix.rust == 'stable' && matrix.target == 'x86_64-unknown-linux-gnu'
      run: |
        cargo install cargo-tarpaulin
        cargo tarpaulin --out Xml --output-dir coverage
        cargo tarpaulin --out Html --output-dir coverage

    - name: Upload coverage to Codecov
      if: matrix.rust == 'stable' && matrix.target == 'x86_64-unknown-linux-gnu'
      uses: codecov/codecov-action@v3
      with:
        file: ./coverage/cobertura.xml
        flags: unittests
        name: codecov-umbrella
        fail_ci_if_error: false
```

### Workflow для документации

```yaml
# .github/workflows/docs.yml
name: Documentation

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  docs:
    name: Build and deploy documentation
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target
        key: ${{ runner.os }}-cargo-stable-${{ hashFiles('**/Cargo.lock') }}

    - name: Build documentation
      run: cargo doc --no-deps --all-features

    - name: Deploy to GitHub Pages
      if: github.ref == 'refs/heads/main'
      uses: peaceiris/actions-gh-pages@v3
      with:
        github_token: ${{ secrets.GITHUB_TOKEN }}
        publish_dir: ./target/doc
        cname: docs.rustbd.org
```

### Workflow для релизов

```yaml
# .github/workflows/release.yml
name: Release

on:
  push:
    tags:
      - 'v*'

jobs:
  release:
    name: Create Release
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target
        key: ${{ runner.os }}-cargo-stable-${{ hashFiles('**/Cargo.lock') }}

    - name: Build for multiple targets
      run: |
        rustup target add x86_64-unknown-linux-gnu
        rustup target add x86_64-apple-darwin
        rustup target add x86_64-pc-windows-msvc
        
        cargo build --release --target x86_64-unknown-linux-gnu
        cargo build --release --target x86_64-apple-darwin
        cargo build --release --target x86_64-pc-windows-msvc

    - name: Create Release
      uses: actions/create-release@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        tag_name: ${{ github.ref }}
        release_name: Release ${{ github.ref }}
        draft: false
        prerelease: false

    - name: Upload Linux binary
      uses: actions/upload-release-asset@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        upload_url: ${{ steps.create_release.outputs.upload_url }}
        asset_path: ./target/x86_64-unknown-linux-gnu/release/rustbd
        asset_name: rustbd-linux-x86_64
        asset_content_type: application/octet-stream

    - name: Upload macOS binary
      uses: actions/upload-release-asset@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        upload_url: ${{ steps.create_release.outputs.upload_url }}
        asset_path: ./target/x86_64-apple-darwin/release/rustbd
        asset_name: rustbd-macos-x86_64
        asset_content_type: application/octet-stream

    - name: Upload Windows binary
      uses: actions/upload-release-asset@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        upload_url: ${{ steps.create_release.outputs.upload_url }}
        asset_path: ./target/x86_64-pc-windows-msvc/release/rustbd.exe
        asset_name: rustbd-windows-x86_64.exe
        asset_content_type: application/octet-stream
```

## 🐳 Docker интеграция

### Dockerfile

```dockerfile
# Dockerfile
FROM rust:1.70-slim as builder

# Установка зависимостей для сборки
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
WORKDIR /usr/src/rustbd

# Копирование файлов зависимостей
COPY Cargo.toml Cargo.lock ./

# Создание пустого lib.rs для кэширования зависимостей
RUN mkdir src && echo "fn main() {}" > src/lib.rs
RUN cargo build --release
RUN rm src/lib.rs

# Копирование исходного кода
COPY src ./src

# Сборка приложения
RUN cargo build --release

# Создание runtime образа
FROM debian:bullseye-slim

# Установка runtime зависимостей
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl1.1 \
    && rm -rf /var/lib/apt/lists/*

# Создание пользователя для безопасности
RUN useradd -r -s /bin/false rustbd

# Создание директорий
RUN mkdir -p /var/lib/rustbd /var/log/rustbd /etc/rustbd
RUN chown -R rustbd:rustbd /var/lib/rustbd /var/log/rustbd /etc/rustbd

# Копирование бинарного файла
COPY --from=builder /usr/src/rustbd/target/release/rustbd /usr/local/bin/rustbd
RUN chown rustbd:rustbd /usr/local/bin/rustbd

# Переключение на пользователя rustbd
USER rustbd

# Открытие порта
EXPOSE 5432

# Установка рабочей директории
WORKDIR /var/lib/rustbd

# Команда по умолчанию
CMD ["rustbd", "--config", "/etc/rustbd/config.toml"]
```

### Docker workflow

```yaml
# .github/workflows/docker.yml
name: Docker

on:
  push:
    branches: [ main ]
    tags: [ 'v*' ]
  pull_request:
    branches: [ main ]

jobs:
  docker:
    name: Build and push Docker image
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v2

    - name: Log in to Docker Hub
      uses: docker/login-action@v2
      with:
        username: ${{ secrets.DOCKER_USERNAME }}
        password: ${{ secrets.DOCKER_PASSWORD }}

    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v4
      with:
        images: your-org/rustbd
        tags: |
          type=ref,event=branch
          type=ref,event=pr
          type=semver,pattern={{version}}
          type=semver,pattern={{major}}.{{minor}}
          type=sha

    - name: Build and push Docker image
      uses: docker/build-push-action@v4
      with:
        context: .
        platforms: linux/amd64,linux/arm64
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max
```

## 🔒 Безопасность

### Security workflow

```yaml
# .github/workflows/security.yml
name: Security

on:
  schedule:
    - cron: '0 2 * * 1'  # Каждый понедельник в 2:00
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  security:
    name: Security checks
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Run cargo audit
      run: |
        cargo install cargo-audit
        cargo audit

    - name: Run cargo deny
      run: |
        cargo install cargo-deny
        cargo deny check

    - name: Run Semgrep
      uses: returntocorp/semgrep-action@v1
      with:
        config: >-
          p/security-audit
          p/secrets
          p/owasp-top-ten
        json: semgrep-results.json

    - name: Upload Semgrep results to GitHub Security tab
      uses: github/codeql-action/upload-sarif@v2
      if: always()
      with:
        sarif_file: semgrep-results.json
```

### Dependency scanning

```yaml
# .github/workflows/dependencies.yml
name: Dependencies

on:
  schedule:
    - cron: '0 0 * * 0'  # Каждое воскресенье в 00:00
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  dependencies:
    name: Check dependencies
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Check for outdated dependencies
      run: |
        cargo install cargo-outdated
        cargo outdated --exit-code 1

    - name: Check for unused dependencies
      run: |
        cargo install cargo-udeps
        cargo udeps

    - name: Create Pull Request for updates
      uses: peter-evans/create-pull-request@v4
      if: github.event_name == 'schedule'
      with:
        token: ${{ secrets.GITHUB_TOKEN }}
        commit-message: 'chore: update dependencies'
        title: 'chore: update dependencies'
        body: |
          This PR updates dependencies to their latest versions.
          
          Automated by GitHub Actions.
        branch: update-dependencies
        delete-branch: true
```

## 📊 Качество кода

### Code quality workflow

```yaml
# .github/workflows/quality.yml
name: Code Quality

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]

jobs:
  quality:
    name: Code quality checks
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Check formatting
      run: cargo fmt --all -- --check

    - name: Clippy check
      run: cargo clippy --all-targets --all-features -- -D warnings

    - name: Check documentation
      run: cargo doc --no-deps --all-features

    - name: Check tests compile
      run: cargo test --no-run --all-targets --all-features

    - name: Run cargo check
      run: cargo check --all-targets --all-features

    - name: Check for dead code
      run: cargo check --all-targets --all-features --message-format=json | grep -q "dead_code" || true
```

### Performance workflow

```yaml
# .github/workflows/performance.yml
name: Performance

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 3 * * 0'  # Каждое воскресенье в 3:00

jobs:
  performance:
    name: Performance benchmarks
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Cache dependencies
      uses: actions/cache@v3
      with:
        path: |
          ~/.cargo/registry
          ~/.cargo/git
          target
        key: ${{ runner.os }}-cargo-stable-${{ hashFiles('**/Cargo.lock') }}

    - name: Run benchmarks
      run: cargo bench --all-features

    - name: Upload benchmark results
      uses: actions/upload-artifact@v3
      with:
        name: benchmark-results
        path: target/criterion
        retention-days: 30

    - name: Comment PR with performance impact
      if: github.event_name == 'pull_request'
      uses: actions/github-script@v6
      with:
        script: |
          const fs = require('fs');
          const path = require('path');
          
          // Анализ результатов бенчмарков
          const benchmarkPath = 'target/criterion';
          if (fs.existsSync(benchmarkPath)) {
            // Логика анализа производительности
            const comment = `## 📊 Performance Analysis
            
            Benchmarks completed successfully. Performance impact analysis will be available soon.
            
            <details>
            <summary>Benchmark Results</summary>
            
            \`\`\`
            Benchmark results have been uploaded as artifacts.
            \`\`\`
            
            </details>`;
            
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: comment
            });
          }
```

## 🚀 Развертывание

### Staging deployment

```yaml
# .github/workflows/deploy-staging.yml
name: Deploy to Staging

on:
  push:
    branches: [ develop ]

jobs:
  deploy-staging:
    name: Deploy to staging environment
    runs-on: ubuntu-latest
    environment: staging

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Build application
      run: cargo build --release

    - name: Deploy to staging server
      uses: appleboy/ssh-action@v0.1.5
      with:
        host: ${{ secrets.STAGING_HOST }}
        username: ${{ secrets.STAGING_USER }}
        key: ${{ secrets.STAGING_SSH_KEY }}
        script: |
          # Остановка текущего сервиса
          sudo systemctl stop rustbd-staging
          
          # Резервное копирование
          sudo cp /opt/rustbd-staging/rustbd /opt/rustbd-staging/rustbd.backup.$(date +%Y%m%d_%H%M%S)
          
          # Обновление бинарного файла
          sudo cp rustbd /opt/rustbd-staging/
          sudo chown rustbd:rustbd /opt/rustbd-staging/rustbd
          sudo chmod +x /opt/rustbd-staging/rustbd
          
          # Запуск сервиса
          sudo systemctl start rustbd-staging
          
          # Проверка статуса
          sudo systemctl status rustbd-staging
          
          # Проверка работоспособности
          sleep 10
          curl -f http://localhost:5432/health || exit 1

    - name: Run smoke tests
      run: |
        # Тесты работоспособности staging окружения
        curl -f ${{ secrets.STAGING_URL }}/health
        curl -f ${{ secrets.STAGING_URL }}/metrics
```

### Production deployment

```yaml
# .github/workflows/deploy-production.yml
name: Deploy to Production

on:
  push:
    tags: [ 'v*' ]

jobs:
  deploy-production:
    name: Deploy to production
    runs-on: ubuntu-latest
    environment: production

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Install Rust toolchain
      uses: actions-rs/toolchain@v1
      with:
        toolchain: stable
        override: true

    - name: Build application
      run: cargo build --release

    - name: Deploy to production servers
      strategy:
        matrix:
          server: [prod-1, prod-2, prod-3]
      
      uses: appleboy/ssh-action@v0.1.5
      with:
        host: ${{ matrix.server }}.${{ secrets.PROD_DOMAIN }}
        username: ${{ secrets.PROD_USER }}
        key: ${{ secrets.PROD_SSH_KEY }}
        script: |
          # Blue-green deployment
          if [ -d "/opt/rustbd-blue" ]; then
            CURRENT="blue"
            NEW="green"
          else
            CURRENT="green"
            NEW="blue"
          fi
          
          # Создание новой версии
          sudo mkdir -p /opt/rustbd-$NEW
          sudo cp rustbd /opt/rustbd-$NEW/
          sudo chown rustbd:rustbd /opt/rustbd-$NEW/rustbd
          sudo chmod +x /opt/rustbd-$NEW/rustbd
          
          # Остановка текущего сервиса
          sudo systemctl stop rustbd-$CURRENT
          
          # Переключение на новую версию
          sudo ln -sf /opt/rustbd-$NEW /opt/rustbd
          sudo systemctl start rustbd-$NEW
          
          # Проверка работоспособности
          sleep 30
          if curl -f http://localhost:5432/health; then
            echo "Deployment successful"
            # Удаление старой версии
            sudo rm -rf /opt/rustbd-$CURRENT
          else
            echo "Deployment failed, rolling back"
            sudo systemctl stop rustbd-$NEW
            sudo ln -sf /opt/rustbd-$CURRENT /opt/rustbd
            sudo systemctl start rustbd-$CURRENT
            exit 1
          fi

    - name: Run production tests
      run: |
        # Тесты работоспособности production окружения
        for server in prod-1 prod-2 prod-3; do
          curl -f https://$server.${{ secrets.PROD_DOMAIN }}/health
          curl -f https://$server.${{ secrets.PROD_DOMAIN }}/metrics
        done

    - name: Notify deployment success
      uses: 8398a7/action-slack@v3
      with:
        status: success
        text: 'Production deployment completed successfully! 🚀'
        webhook_url: ${{ secrets.SLACK_WEBHOOK }}
```

## 📈 Мониторинг и метрики

### Metrics collection

```yaml
# .github/workflows/metrics.yml
name: Collect Metrics

on:
  schedule:
    - cron: '*/15 * * * *'  # Каждые 15 минут

jobs:
  metrics:
    name: Collect system metrics
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Collect GitHub metrics
      uses: actions/github-script@v6
      with:
        script: |
          const { data: issues } = await github.rest.issues.listForRepo({
            owner: context.repo.owner,
            repo: context.repo.repo,
            state: 'open',
            per_page: 100
          });
          
          const { data: prs } = await github.rest.pulls.list({
            owner: context.repo.owner,
            repo: context.repo.repo,
            state: 'open',
            per_page: 100
          });
          
          const { data: commits } = await github.rest.repos.listCommits({
            owner: context.repo.owner,
            repo: context.repo.repo,
            per_page: 100
          });
          
          const metrics = {
            timestamp: new Date().toISOString(),
            open_issues: issues.length,
            open_prs: prs.length,
            recent_commits: commits.length,
            repo_size: context.payload.repository.size,
            stars: context.payload.repository.stargazers_count,
            forks: context.payload.repository.forks_count
          };
          
          console.log('Metrics:', JSON.stringify(metrics, null, 2));
          
          // Сохранение метрик в файл
          const fs = require('fs');
          fs.writeFileSync('metrics.json', JSON.stringify(metrics, null, 2));

    - name: Upload metrics
      uses: actions/upload-artifact@v3
      with:
        name: metrics-${{ github.run_id }}
        path: metrics.json
        retention-days: 90
```

## 🔧 Конфигурация

### GitHub Secrets

Для работы CI/CD необходимо настроить следующие секреты:

```bash
# Docker Hub
DOCKER_USERNAME=your-username
DOCKER_PASSWORD=your-password

# SSH ключи для серверов
STAGING_SSH_KEY=-----BEGIN OPENSSH PRIVATE KEY-----
PROD_SSH_KEY=-----BEGIN OPENSSH PRIVATE KEY-----

# Серверы
STAGING_HOST=staging.example.com
STAGING_USER=deploy
PROD_DOMAIN=example.com
PROD_USER=deploy

# Уведомления
SLACK_WEBHOOK=https://hooks.slack.com/services/...

# Codecov
CODECOV_TOKEN=your-codecov-token
```

### Environment protection rules

```yaml
# .github/environments/production.yml
name: production
protection_rules:
  - required_reviewers:
      count: 2
      users: [admin1, admin2]
  - required_status_checks:
      strict: true
      contexts:
        - "test"
        - "security"
        - "quality"
        - "performance"
  - deployment_branch_policy:
      protected_branches: true
      custom_branch_policies: []
```

## 📚 Лучшие практики

### Общие принципы

1. **Автоматизация**: Автоматизируйте все повторяющиеся процессы
2. **Безопасность**: Используйте секреты для чувствительных данных
3. **Мониторинг**: Отслеживайте все этапы CI/CD
4. **Тестирование**: Запускайте тесты перед каждым развертыванием
5. **Откат**: Имейте план отката для критических изменений

### Организация workflows

1. **Разделение ответственности**: Разделяйте CI и CD на отдельные workflows
2. **Кэширование**: Используйте кэширование для ускорения сборки
3. **Параллелизация**: Запускайте независимые задачи параллельно
4. **Уведомления**: Настройте уведомления о важных событиях

### Развертывание

1. **Blue-green**: Используйте blue-green развертывание для production
2. **Canary**: Тестируйте новые версии на части трафика
3. **Rollback**: Имейте быстрый механизм отката
4. **Мониторинг**: Отслеживайте метрики после развертывания

## 🔗 Дополнительные ресурсы

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Rust CI/CD Best Practices](https://rust-lang.github.io/rustc-guide/ci.html)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [Security Best Practices](https://securitylab.github.com/research/)

Следуя этим рекомендациям, вы создадите надежную и эффективную систему CI/CD для RustBD.
