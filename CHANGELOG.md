Changelog
=========


(unreleased)
------------
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


