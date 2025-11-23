# Release Process

This guide explains how to create and publish releases for Cleared. The release process is automated through GitHub Actions and handles version bumping, testing, changelog generation, GitHub releases, and PyPI publishing.

## Overview

The release workflow automates the entire release process:

1. **Version Bumping** - Updates version in both `VERSION` file and `pyproject.toml`
2. **Quality Checks** - Runs full CI pipeline (lint, format, test, docs)
3. **Changelog Generation** - Creates/updates `CHANGELOG.md`
4. **Git Tagging** - Creates and pushes version tag
5. **GitHub Release** - Creates a GitHub release with release notes
6. **PyPI Publishing** - Publishes package to PyPI

## Creating a Release

Releases are created manually via GitHub Actions:

1. **Navigate to Actions**
   - Go to your repository on GitHub
   - Click on the **Actions** tab
   - Select **Release** workflow from the left sidebar

2. **Trigger the Workflow**
   - Click **Run workflow** button
   - Select the version bump type:
     - **patch**: Bug fixes (0.1.0 → 0.1.1)
     - **minor**: New features (0.1.0 → 0.2.0)
     - **major**: Breaking changes (0.1.0 → 1.0.0)
   - Choose whether to:
     - ✅ **Publish to PyPI** (default: true)
     - ✅ **Create GitHub release** (default: true)
   - Click **Run workflow**

3. **Monitor the Workflow**
   - Watch the workflow progress in the Actions tab
   - The workflow will automatically:
     - Bump the version
     - Run all tests
     - Generate changelog
     - Create git tag
     - Create GitHub release
     - Publish to PyPI

## Workflow Steps Explained

### Job 1: Prepare Release

1. **Checkout code** - Gets the latest code
2. **Setup environment** - Installs Python, Task, Poetry
3. **Bump version** - Uses `task version-bump` to update version in both `VERSION` and `pyproject.toml`
4. **Run CI checks** - Runs full test suite (`task ci-local`) including:
   - Linting
   - Formatting checks
   - Unit tests
   - Documentation checks
5. **Generate changelog** - Creates `CHANGELOG.md` using gitchangelog
6. **Build packages** - Creates wheel and source distribution
7. **Commit changes** - Commits version and changelog updates
8. **Create tag** - Creates and pushes git tag (e.g., `v0.1.1`)

### Job 2: Create GitHub Release

1. **Download artifacts** - Gets built packages from previous job
2. **Generate release notes** - Creates release notes from changelog or git log
3. **Create release** - Creates GitHub release with:
   - Tag name
   - Release notes
   - Distribution files as assets

### Job 3: Publish to PyPI

1. **Download artifacts** - Gets built packages
2. **Publish** - Uploads to PyPI using Poetry

## Version Numbering

The project uses [Semantic Versioning](https://semver.org/):

- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **MAJOR**: Breaking changes that require users to modify their code
- **MINOR**: New features that are backward compatible
- **PATCH**: Bug fixes that are backward compatible

### Examples

- `0.1.0` → `0.1.1` (patch: bug fix)
- `0.1.0` → `0.2.0` (minor: new feature)
- `0.1.0` → `1.0.0` (major: breaking change)

## Local Release Process (Alternative)

If you prefer to release manually without using GitHub Actions:

```bash
# 1. Bump version
task version-bump patch  # or minor, major

# 2. Run tests
task ci-local

# 3. Generate changelog
task changelog

# 4. Build packages
task build

# 5. Review changes
git diff

# 6. Commit and tag
git add VERSION pyproject.toml CHANGELOG.md
git commit -m "chore: release v$(cat VERSION)"
git tag -a "v$(cat VERSION)" -m "Release v$(cat VERSION)"
git push origin main --tags

# 7. Create GitHub release manually (via GitHub UI)

# 8. Publish to PyPI
poetry publish --username __token__ --password $PYPI_API_TOKEN
```

## Troubleshooting

### "Publishing failed" Error

If you see a publishing error, it might be because:
- The version already exists on PyPI
- PyPI API token is invalid or expired
- Network issues

**Solution:** Check PyPI to see if the version exists. If it does, you may need to bump the version again.

### "No changes to commit" Warning

This is normal if the version was already bumped manually. The workflow will continue.

### Changelog Not Generated

If `gitchangelog` fails, the workflow will continue. You can manually update `CHANGELOG.md` and commit it.

### Tag Already Exists

If a tag already exists, the workflow will fail at the tagging step. Delete the tag first:

```bash
git tag -d v0.1.1  # Delete local tag
git push origin :refs/tags/v0.1.1  # Delete remote tag
```

### CI Checks Fail

If CI checks fail during the release:
1. Fix the issues locally
2. Commit and push the fixes
3. Re-run the release workflow

## Best Practices

1. **Always run `task ci-local` locally** before triggering a release
2. **Review the changelog** after generation and update if needed
3. **Test the release** in a test PyPI repository first (optional)
4. **Use patch for bug fixes**, **minor for features**, **major for breaking changes**
5. **Keep release notes clear and descriptive**
6. **Ensure all tests pass** before releasing
7. **Review all changes** in the release commit before pushing

## Security Notes

- Never commit PyPI tokens to the repository
- Use GitHub Secrets for all sensitive information
- Consider using GitHub Environments for additional protection
- Review all changes before releasing
- Use 2FA on your PyPI account

## Verifying a Release

After a release is published, verify it:

1. **Check PyPI**: Visit [https://pypi.org/project/cleared](https://pypi.org/project/cleared)
2. **Check GitHub Releases**: Visit the Releases page on GitHub
3. **Test Installation**: Try installing the new version:
   ```bash
   pip install --upgrade cleared
   ```

## Support

If you encounter issues:

1. Check the workflow logs in GitHub Actions
2. Verify all secrets are configured correctly
3. Ensure you have the necessary permissions
4. Review this guide for common issues
5. Open an issue on GitHub if problems persist

## Environment Setup

Before creating releases, you need to set up the required environment and credentials:

### 1. PyPI API Token Setup

Before you can publish to PyPI, you need to set up a PyPI API token:

1. Go to [PyPI Account Settings](https://pypi.org/manage/account/)
2. Navigate to "API tokens" section
3. Click "Add API token"
4. Give it a name (e.g., "cleared-release-bot")
5. Set scope to "Entire account" (or specific project)
6. Copy the token (it starts with `pypi-`)

### 2. GitHub Secrets Configuration

Add the PyPI token as a GitHub secret:

1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `PYPI_API_TOKEN`
5. Value: Paste your PyPI API token
6. Click **Add secret**

### 3. GitHub Environment (Optional but Recommended)

For better security and approval workflows:

1. Go to **Settings** → **Environments**
2. Create a new environment named `pypi`
3. Add the `PYPI_API_TOKEN` secret to this environment
4. Optionally, add required reviewers for production releases

---

For more information about the release workflow, see the [Release Workflow Guide](../.github/RELEASE_WORKFLOW.md) in the repository.

