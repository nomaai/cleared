### HOW TO USE THIS TEMPLATE

## Using This Template


> **DO NOT FORK** this is meant to be used from **[Use this template](https://github.com/new?owner=nomaai&template_name=noma_python_template&template_owner=nomaai)** feature.

1. Click on **[Use this template](https://github.com/new?owner=nomaai&template_name=noma_python_template&template_owner=nomaai)**
3. Give a name to your project  
   (e.g. `my_awesome_project` recommendation is to use all lowercase and underscores separation for repo names.)
3. Wait until the first run of CI finishes  
   (Github Actions will process the template and commit to your new repo)
4. Read the file [CONTRIBUTING.md](CONTRIBUTING.md)
5. Then clone your new project and happy coding!
6. Run the initialization script to setup the project for your fresh new project

> **NOTE**: **WAIT** until first CI run on github actions before cloning your new project.

## Inititialization Script Explanation

This script will initialize the project's name, setup.py, readme.md file, delete template related .md files

### Usage

'''bash

make init project_name=<your_project_name> github_url=<github repo url address>

'''

Using `source` allows the script to be run in the current shell session without terminating the terminal. The script will prompt for a project description during execution, check the `VERSION` file, and rename the project directory if needed.

### Help Function

-   **`display_help`**: Prints usage information.

### Check for `--help` Option

-   The script checks if the first argument is `--help`. If it is, the `display_help` function is called, and the script exits with a status of 0.

### Check for Required Arguments

-   The script verifies if both `project_name` and `github_url` are provided. If either is missing, it prints an error message, displays the help information, and exits with a status of 1.

### Check the `VERSION` File

-   The script checks if the `VERSION` file exists. If it doesn't, an error message is printed, and the script exits with a status of 1.
-   If the `VERSION` file exists, the script reads its content and checks if it is `0.0.0`. If not, an error message is printed, and the script exits with a status of 1.

### Assign Variables

-   The provided `project_name` and `github_url` are assigned to variables.

### Prompt for Project Description

-   The script prompts the user to enter a short description for the project and assigns it to the `PROJECT_DESCRIPTION` variable.

### Check `setup.py` Existence

-   The script checks if `setup.py` exists. If not, it prints an error message and exits with a status of 1.

### Update `setup.py`

-   The script uses `sed` to find and replace all occurrences of `{project_name}` with the provided `project_name` in `setup.py`.
-   Similarly, it replaces all occurrences of `{url}` with the provided `github_url`.
-   It also replaces all occurrences of `{project_description}` with the provided `PROJECT_DESCRIPTION`.

### Inform User

-   The script prints messages indicating that all occurrences of `{project_name}`, `{url}`, and `{project_description}` have been updated in `setup.py`.

### Change Directory Name

-   The script assumes the current directory name is `project_name` and renames the directory to the new `project_name`.

### Update `VERSION` File

-   The script updates the content of the `VERSION` file to `0.1.0`.