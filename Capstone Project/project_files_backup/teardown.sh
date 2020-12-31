#!/bin/bash

## From bash manpage,
## Aliases are not expanded when the shell is not interactive,
## unless the expand_aliases shell option is set using shopt.
shopt -s expand_aliases
source ~/.bashrc

echo "Shutting down AWS services"
echo "=========================="
echo "AWS EMR:"
echo "--------"
emr-terminate
emr-list

echo -e "\nAWS Redshift:"
echo "-------------"
redshift-terminate
redshift-list

echo -e "\nDone."