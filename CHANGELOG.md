Changelog
=========


(unreleased)
------------
- Added `skip_missing_tables` configuration option (defaults to `true`) that allows
  the engine to skip tables that don't have a corresponding input file instead of
  raising an error. Missing tables are logged with a warning and marked as "skipped"
  in the results. Set to `false` if you want the engine to fail when a table file
  is missing. This is useful for multi-table pipelines where some tables may be
  optional or not yet available.

- Fixed formating (#56) [salimnoma]

  * Fixed formating

  * Updated img path in readme.md

  * Changed images back to img tag

  * Fixed formatting

  * Updated poetry lock
- Improvements to project structure as ppypi package (#55) [salimnoma]

  * fixed project organization

  * Updated Poetry

  * Fixed parent path in path
- Revert "Fixed project organization as a pypi package" (#54)
  [salimnoma]

  This reverts commit ac06bdc547932642bcd114d11a65444f94096623.
- Fixed project organization as a pypi package. [salimnoma]

  * Fixed project organization as a pypi package including __version__

  * Updated Poetry lock file
- Update release workflow to release branch on failure (#52) [salimnoma]

  * Updated PR Creation

  * Bump patch version

  * add build mode to task verify-deps

  * Fixed taskfile to properly pass args mode=build  task verify when running as cmds in taskfile

  * Added task verify-deps-build

  * Updated the workflow to fix the README.md verification step

  * Updated pypi to fetch the branch for upgrade
- Chore: bump version to 0.4.6 (#51) [github-actions[bot], github-
  actions[bot]]
- Change upload pypi to rebuild (#50) [salimnoma]

  * Updated PR Creation

  * Bump patch version

  * add build mode to task verify-deps

  * Fixed taskfile to properly pass args mode=build  task verify when running as cmds in taskfile

  * Added task verify-deps-build

  * Updated the workflow to fix the readme.md verification step

  * Updated pypi to fetch the branch for upgrade
- Revert "Change upload pypi to rebuild (#48)" (#49) [salimnoma]

  This reverts commit 46c5644afa9c3e7a3c0a62d7d0352b550e36493f.
- Change upload pypi to rebuild (#48) [salimnoma]

  * Updated PR Creation

  * Bump patch version

  * add build mode to task verify-deps

  * Fixed taskfile to properly pass args mode=build  task verify when running as cmds in taskfile

  * Added task verify-deps-build

  * Updated the workflow to fix the readme.md verification step
- Revert "Change upload pypi to rebuild (#46)" (#47) [salimnoma]

  This reverts commit 4af43a564e5ef8a486b956a48540934d661ef9e5.
- Change upload pypi to rebuild (#46) [salimnoma]

  * Updated PR Creation

  * Bump patch version

  * add build mode to task verify-deps

  * Fixed taskfile to properly pass args mode=build  task verify when running as cmds in taskfile

  * Added task verify-deps-build
- Revert "Change upload pypi to rebuild (#44)" (#45) [salimnoma]

  This reverts commit 5ce16b6964af386941d4f3f66e1b9e1ed2bb584e.
- Change upload pypi to rebuild (#44) [salimnoma]

  * Updated PR Creation

  * Bump patch version

  * add build mode to task verify-deps

  * Fixed taskfile to properly pass args mode=build  task verify when running as cmds in taskfile
- Revert "Change upload pypi to rebuild (#42)" (#43) [salimnoma]

  This reverts commit 115d5ed23232b78f6dfd7c1d4f5b9b87e3e9fb22.
- Change upload pypi to rebuild (#42) [salimnoma]

  * Updated PR Creation

  * Bump patch version

  * add build mode to task verify-deps
- Revert "Change upload pypi to rebuild (#40)" (#41) [salimnoma]

  This reverts commit b0596313c6b133ee0a7ff940df1fcee56aaa7bd9.
- Change upload pypi to rebuild (#40) [salimnoma]

  * Updated PR Creation

  * Bump patch version
- Chore: bump version to 0.4.5 (#39) [github-actions[bot], github-
  actions[bot]]
- Prevent poetry build to use wrong readme (#38) [salimnoma]

  * Updated PR Creation

  * Bump patch version
- Revert "Updated PR Creation (#36)" (#37) [salimnoma]

  This reverts commit 0c03d835d098cca09f659179d5c3461d19dede3f.
- Updated PR Creation (#36) [salimnoma]
- Bump version patch 0.4.3 (#35) [salimnoma]
- Updated pyproject.toml for pypi upload(#34) [salimnoma]

  * Updated pyproject.toml

  * Updated creating code update version PR

  * Updated poetry.lock

  * Updated license
- Fixed issues in release workflow (#33) [salimnoma]
- Updated version to 0.4.2 (#32) [Salim Malakouti, salimnoma]
- Implement realease fail rollback and post success jobs (#31)
  [salimnoma]

  * Implemented success and rollback jobs

  * Fixed issues

  * Added retry to onstal os dependecies
- Revert "Implemented success and rollback jobs (#29)" (#30) [salimnoma]

  This reverts commit 85bf9f89ee5ce5260c52f4bf47dd7a729686686e.
- Implemented success and rollback jobs (#29) [salimnoma]
- Added confirmation steps (#28) [salimnoma]

  * Added confirmation steps

  * changed twine install to use task setup-env
- Fixed upload pypi (#27) [Salim Malakouti, salimnoma]

  * Fixed upload pypi

  * updated poetry lock

  ---------
- Revert "Fixed upload pypi (#25)" (#26) [salimnoma]

  This reverts commit 9f941384bdf92b43652a0b940b6eae851018d7a6.
- Fixed upload pypi (#25) [salimnoma]
- Switch pypi publish to use twine (#23) [salimnoma]
- Add taskfile install to publish_pypi (#22) [Salim Malakouti,
  salimnoma]
- Added install task to pypi workflow (#21) [salimnoma]
- Fix release workflow permission error (#20) [salimnoma]

  * removed CI workflow from push main

  * Fixed the taskfile command for version-bump

  * Fixed the taskfile command for version-bump
- Fix issue with release workflow (#19) [salimnoma]

  * removed CI workflow from push main

  * Fixed the taskfile command for version-bump
- Fixed bump version (#18) [salimnoma]
- Updated license (#17) [Salim Malakouti, salimnoma]
- Updated documentations (#16) [salimnoma]

  * Updated documentation

  * Updated release docs and process
- Fix verify casting issue (#15) [Salim Malakouti, salimnoma]

  * Improved the logging

  * Changed verify to work based on transformers

  * Changed verify to work based on transformers

  * Implement a transformer oriented verify command

  * updated poetry

  * addressed issues pointed out by Zenable

  ---------
- Improved the logging (#13) [salimnoma]

  * Improved the logging

  * updated poetry

  * addressed issues pointed out by Zenable
- Implemented verify and report (#12) [Salim Malakouti, salimnoma]
- Adaopted the code to fully use hydra compose (#11) [salimnoma]
- Implemented reverse functionality (#10) [salimnoma]

  * Implemented reverse functionality

  * Fixed issues in argument validation
- Implemented cleared test (#9) [salimnoma]
- Implemented describe config command tool (#8) [Salim Malakouti,
  salimnoma]

  * Implemented desribe config command tool

  * Added docs for describe

  * removed lazy importing

  * Fixed index access issue

  ---------
- Implemented linting, formating and setup commands. [salimnoma]
- Remove DateTimeDeIdentifier's deid_config and pass global_deid_config
  to all transformers (#6) [Salim Malakouti, salimnoma]

  * made global deid config an optional config to all transformers

  * removed 3.14 from supported versions

  ---------
- Implemented release workflow (#4) [Salim Malakouti, salimnoma]

  * Implemented release workflow

  * update poetry.lock

  * Fixed dependencies

  * Addressed coding style issues

  * Restricted to 3.10+

  * updated poetry.ock

  * removed 3.14 from the supported versions due to dependency lack of support

  ---------
- Implemented filtered transformers and support for value casting (#3)
  [Salim Malakouti, salimnoma]

  * Implemented filtered transformers

  * Fixed minor code issues

  * Fixed unit test error and ignore lint issue

  ---------
- Implement id and time deidentification (#2) [Salim Malakouti,
  salimnoma]

  * initial commit

  * initial commit

  * initial commit

  * updated PR_TEMPLATE

  * Added documentation for contributing

  * Fixed CI to install os dependecies

  * Fixed install os deps error on ubuntu

  * Added fswatch to install_os_deps and removed installing from verify_deps

  * Fixed CI to skip installed if cache hit and run setup-dev to have dev dependencies

  * Fixed CI step format

  * Fixed issues with doc generation

  * Implemented standard transformers and engine

  * ignored .venv from yamllint

  * Handled older python version in error untitest

  * fix future syntax support in 3.8+

  * updated syntax of pyproject.toml

  * Handled type hinting for 3.8+

  * limit CI to 3.9+

  * Handled type hinting for 3.8+

  * Change codes to oextend 3.9 support

  * Fixed unit test for 3.9

  ---------
- Implement the project structure (#1) [Salim Malakouti, salimnoma]

  * initial commit

  * initial commit

  * initial commit

  * updated PR_TEMPLATE

  * Added documentation for contributing

  * Fixed CI to install os dependecies

  * Fixed install os deps error on ubuntu

  * Added fswatch to install_os_deps and removed installing from verify_deps

  * Fixed CI to skip installed if cache hit and run setup-dev to have dev dependencies

  * Fixed CI step format

  * Fixed issues with doc generation

  ---------
- Initial commit. [salimnoma]


