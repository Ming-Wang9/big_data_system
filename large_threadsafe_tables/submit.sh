#!/bin/sh

set -e

echo "Submitting for the repository — $CI_REPOSITORY_URL"
BASE_URL=$(echo "$CI_REPOSITORY_URL" | sed -n 's|.*@\(.*\)\.git$|\1|p')
PROJECT=$(echo "$CI_REPOSITORY_URL" | awk -F'/' '{for (i=1; i<=NF; i++) if ($i ~ /^p[0-9]+$/) print $i}')
RESPONSE=$(curl -fsS -X POST -H "Content-Type: application/json" -d "{\"repo_url\": \"https://$BASE_URL\",\"project\": \"$PROJECT\"}" http://cs544-autograder.cs.wisc.edu:"$DEPLOYED_AUTOBADGER_PORT"/register)
echo $RESPONSE | jq .