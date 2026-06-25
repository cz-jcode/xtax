# Git Workflow for Cline

Purpose: enforce a consistent Rust-style PR workflow for all changes. Every change must go through a feature branch, a conventional commit, and a pull request.

## Rules (MUST)

1. **Always create a new branch** before committing.
   - Branch names must be kebab-case: `add-docs-ci-job`, `fix-blob-encrypt-panic`, `ci-pin-actions`.
   - Use a descriptive, short name that reflects the change.

2. **Commit messages must follow conventional commits** with a Rust-style prefix:
   - `feat:` – new feature
   - `fix:` – bug fix
   - `docs:` – documentation only
   - `ci:` – CI/CD changes
   - `chore:` – maintenance, deps, tooling
   - `refactor:` – code change that neither fixes a bug nor adds a feature
   - `style:` – formatting, whitespace (no code change)
   - `test:` – adding or updating tests
   - `perf:` – performance improvement
   - `build:` – build system or external dependencies
   - `revert:` – revert a previous commit

3. **Push the branch** to `origin`.

4. **Create a pull request** via `gh pr create`:
   - Use the commit message as the PR title.
   - Fill the PR body with a summary of changes.
   - Use `--base main`.
   - Reference related issues if applicable.

## Example workflow

```bash
# 1. Create branch
git checkout -b add-docs-ci-job

# 2. Stage and commit
git add .github/workflows/ci.yml
git commit -m "ci: add docs job to CI workflow"

# 3. Push
git push -u origin add-docs-ci-job

# 4. Create PR
gh pr create \
  --base main \
  --title "ci: add docs job to CI workflow" \
  --body "Build rustdoc for xtax-encryption, xtax-blob-storage, and xtax with --all-features --no-deps, treating warnings as errors."
```

## Reference

- Conventional commits: <https://www.conventionalcommits.org/>
- Rust commit style often matches conventional commits with lowercase, no trailing punctuation.