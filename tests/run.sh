#/bin/bash
set -e

if [ -x "$(command -v docker-compose)" ]; then
  # docker-compose may be installed as a separate binary
  docker-compose up --build -d
elif [ -x "$(command -v docker)" ]; then
  # or as a plugin
  docker compose up --build -d
elif [ -x "$(command -v podman-compose)" ]; then
  # Or you may be using Podman instead
  podman-compose up --build -d
else
  echo "docker-compose or podman-compose must be installed to use this script"
fi

if [[ ! $CI ]]; then
    source ~/.nvm/nvm.sh

    nvm install
    nvm use
    npm i
    npm test
fi
