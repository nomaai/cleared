#!/bin/bash

# Function to display help message
display_help() {
    echo "Usage: $0 [OPTIONS] <project_name> <github_url>"
    echo
    echo "Options:"
    echo "  --help    Show this help message"
    echo
    echo "Positional Arguments:"
    echo "  project_name       The new name for the project"
    echo "  github_url         The URL to the github repository of the new project"
    echo
}

# Check for --help option
if [ "$1" == "--help" ]; then
    display_help
    return 0
fi

# Check if the project name and GitHub URL are provided
if [ -z "$1" ] || [ -z "$2" ]; then
  echo "Error: Both project name and GitHub URL must be provided."
  display_help
  return 1
fi

# Check the VERSION file
if [ ! -f VERSION ]; then
  echo "Error: VERSION file not found!"
  return 1
fi

VERSION_CONTENT=$(cat VERSION)
if [ "$VERSION_CONTENT" != "0.0.0" ]; then
  echo "Error: Project is already initialized."
  return 1
fi

# Assign the provided project name and GitHub URL to variables
PROJECT_NAME=$1
GITHUB_URL=$2

# Prompt user for project description
read -p "Enter a short description for the project: " PROJECT_DESCRIPTION

# Define the setup.py file path
SETUP_PY_FILE="setup.py"

# Check if the setup.py file exists
if [ ! -f "$SETUP_PY_FILE" ]; then
  echo "Error: $SETUP_PY_FILE not found!"
  return 1
fi

# Use sed to update all occurrences of project_name in setup.py
sed -i '' -e "s/project_name/$PROJECT_NAME/g" "$SETUP_PY_FILE"

# Use sed to update all occurrences of github_url in setup.py
sed -i '' -e "s|github_url|$GITHUB_URL|g" "$SETUP_PY_FILE"

# Use sed to update all occurrences of project_description in setup.py
sed -i '' -e "s|project_description|$PROJECT_DESCRIPTION|g" "$SETUP_PY_FILE"

# Inform the user of the change
echo "Updated all occurrences of project_name in $SETUP_PY_FILE to \"$PROJECT_NAME\""
echo "Updated all occurrences of github_url in $SETUP_PY_FILE to \"$GITHUB_URL\""
echo "Updated all occurrences of project_description in $SETUP_PY_FILE to \"$PROJECT_DESCRIPTION\""

# Change the name of the project directory
mv "project_name" "$PROJECT_NAME"
echo "Renamed project directory from 'project_name' to '$PROJECT_NAME'"

# Delete the docs/template directory
if [ -d "docs/template" ]; then
  rm -r "docs/template"
  echo "Deleted the directory 'docs/template'"
fi

# Remove the line containing "### [How To Use This Template](template/use_this_template.md)" from docs/index.md
if [ -f "docs/index.md" ]; then
  sed -i '' '/### \[How To Use This Template\](template\/use_this_template\.md)/d' "docs/index.md"
  echo "Removed the line containing '### [How To Use This Template](template/use_this_template.md)' from docs/index.md"
fi

# Delete the existing README.md file and create a new one with the specified content
if [ -f "README.md" ]; then
  rm "README.md"
  echo "Deleted the existing README.md file"
fi

# Write the new README.md content
echo "# $PROJECT_NAME

$PROJECT_DESCRIPTION

## Template Project:
Template used to generate this project is [noma_python_template](https://github.com/nomaai/noma_python_template)

" > README.md
echo "Created a new README.md with the project name, description, and template information"

# Update the line "site_name: project_name" in mkdocs.yaml
if [ -f "mkdocs.yaml" ]; then
  sed -i '' -e "s|site_name: {project_name}|site_name: $PROJECT_NAME|" "mkdocs.yaml"
  echo "Updated the site_name in mkdocs.yaml to '$PROJECT_NAME'"
fi

# Update VERSION file
echo "0.1.0" > VERSION