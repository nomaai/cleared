#!/bin/bash
# ------------------------------------
# This script runs jupyter Lab remotely
# ------------------------------------


# Define variables
IP=$(ip -4 addr | sed -ne 's|^.* inet \([^/]*\)/.* scope global.*$|\1|p' | head -1)
NOTEBOOK_DIR=${NOTEBOOK_DIR:-"$(pwd)"}


# Check if jupyter is available
if ! command -v jupyter &> /dev/null
then
    echo "Looks like jupyter is not available. Run the following command to setup the environment:"
    echo
    echo "task setup-env"
    echo "poetry shell"
    exit
fi


# Check if password is set first
if [ -f "~/.jupyter/jupyter_server_config.json" ]; then
    echo "Jupyter Lab config file was not generated!"
    echo "First run the following commands to create config and password:"
    echo
    echo "task install-jupyter"
    echo "poetry run jupyter server --generate-config"
    echo "poetry run jupyter server password"
    echo 
    exit
fi
PASSWORD_STR=`cat ~/.jupyter/jupyter_server_config.json  | grep "password" | sed  's/^[ ]*"password":[ ]*"\(.*\)".*$/\1/'`
if [ ${#PASSWORD_STR} -le 5 ]; then 
  echo "Jupyter Lab config file existed but password is not set!" ; 
  echo "First run the following commands to create config and password:"
  echo
  echo "poetry run jupyter server password"
  echo ""
  exit
fi

unset PASSWORD_STR

echo "You are running Jupyter Lab with remote access to $IP and note book path $NOTEBOOK_DIR"
while true; do
    read -p "Are you sure? [Y/n]" -n 1 -r yn
    case $yn in
        [Yy]* ) jupyter lab --no-browser --ip "$IP" --notebook-dir $NOTEBOOK_DIR; break;;
        [Nn]* )  echo; break;;
        * ) echo "Please answer yes or no."; echo;;
    esac
done