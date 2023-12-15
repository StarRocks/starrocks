#!/bin/bash

declare -a branches=("branch-3.2" "branch-3.1" "branch-3.0" "branch-2.5" "branch-2.3" "branch-2.2" "branch-2.1")

export DISABLE_VERSIONING=true
cd starrocks/docs

for branch in "${branches[@]}"
do
	echo "Working on $branch"
	git switch $branch
	git pull
	rm -rf docusaurus/docs
	cp -r en docusaurus/docs
	rm -rf docusaurus/i18n/zh/docusaurus-plugin-content-docs/current
	mkdir -p docusaurus/i18n/zh/docusaurus-plugin-content-docs
	cp -r zh docusaurus/i18n/zh/docusaurus-plugin-content-docs/current
	sed -i "s/: 'ignore'/: 'warn'/g" docusaurus/docusaurus.config.js
	cd docusaurus
	yarn install --frozen-lockfile
	yarn clear
	yarn build > ${branch}.log 2>&1
	cd ../
done
