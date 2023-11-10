#!/bin/bash

# set the env var to build current only
export DISABLE_VERSIONING=true

# install / update 
yarn install --frozen-lockfile
yarn start --locale ${DOCS_LOCALE}
