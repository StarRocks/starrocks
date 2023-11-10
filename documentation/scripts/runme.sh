#!/bin/bash

# set the env var to build current only
export DISABLE_VERSIONING=true

# install / update 
yarn install --frozen-lockfile
yarn build && yarn serve
