#/bin/bash

docker compose up --build -d

if [[ ! $CI ]]; then
    nvm use v20.11.0
    npm i
    npm test
fi
