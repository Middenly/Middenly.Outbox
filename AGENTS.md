# Agent Workflow Rules

These rules apply to ALL tasks in this repository.

## After Every Code Change

After implementing any functional change, follow this checklist in order:

### 1. Build
```
dotnet build --configuration Release
```
The build must succeed with zero errors and zero warnings.

### 2. Run All Tests
```
dotnet test --configuration Release --verbosity normal
```
All tests must pass. If a test fails — fix it before moving on.

### 3. Check Test Coverage
If the new code introduces new logic (classes, methods, branches) and there are no tests for it — write them. Prefer:
- **Unit tests** for pure logic (models, options, serialization)
- **Integration tests with Testcontainers** for database and Kafka interactions

### 4. Update Documentation
If the change affects:
- **Public API** (new methods, changed signatures, new options) → update `docs/guide/`
- **Configuration** (new/changed options) → update `docs/guide/configuration.md`
- **Database schema** (new columns, indexes) → update `docs/guide/schema.md` and `docs/guide/postgresql.md`
- **Behavior** (new features, changed defaults) → update `docs/introduction/what-is-it.md` and relevant guide pages

### 5. Verify Docs Build (if docs changed)
```
cd docs
npm install
npm run build
```

## Commit Convention

Do NOT commit unless the user explicitly asks. When committing:
- One logical change per commit
- Message format: `<type>: <description>`
- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

## Test Rules

- Never skip or delete an existing test to make the build pass — fix the code or the test
- Integration tests use Testcontainers (PostgreSQL + Kafka) — require Docker
- Unit tests must run without Docker
- Test class naming: `{ClassUnderTest}Tests`
- Test method naming: `{Method}_{Scenario}_{ExpectedResult}`
