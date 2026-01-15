mkdir -p docusaurus/docs

cp -r enterprise-temp/docs/enterprise/docusaurus/* docusaurus
cp -r enterprise-temp/docs/en/* docusaurus/docs/
rm -rf docusaurus/docs/deployment
rm docusaurus/docs/introduction/what_is_starrocks.md
rm docusaurus/docs/introduction/introduction.mdx
cp -r enterprise-temp/docs/enterprise/docs/* docusaurus/docs/
cp enterprise-temp/docs/en/deployment/plan_cluster.md docusaurus/docs/deployment/15_plan_cluster.md
rm -rf docusaurus/docs/quick_start
rm -rf docusaurus/docs/cover_pages
rm -rf docusaurus/docs/project_help
rm -rf docusaurus/docs/developers
rm -rf docusaurus/docs/release_notes
rm -rf docusaurus/docs/ecosystem_release
rm docusaurus/docs/data_source/icebergtutorial.mdx
rm docusaurus/docs/introduction/StarRocks_intro.md
cp -r enterprise-temp/docs/enterprise/docs/introduction/* docusaurus/docs/introduction/
rm docusaurus/docs/administration/stargo.md
rm docusaurus/docs/administration/administration.mdx
rm -rf docusaurus/docs/integrations/other_integrations
rm docusaurus/docs/integrations/streaming.mdx
rm docusaurus/docs/integrations/airflow.md
rm docusaurus/docs/loading/Json_loading.md
rm docusaurus/docs/loading/loading.mdx
rm docusaurus/docs/loading/loading_introduction/loading_overview.mdx
rm docusaurus/docs/loading/objectstorage.mdx
rm docusaurus/docs/unloading/unloading.mdx

cd docusaurus/

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
    find docs -name "*.md*" | xargs -d "\n" sed -i "s/displayed_sidebar:.*//"
elif [[ "$OSTYPE" == "darwin"* ]]; then
    find docs -name "*.md*" | xargs -d "\n" sed -i '' "s/displayed_sidebar:.*//"
fi

find . -regex '.*\.\(mdx\|md\)$' \
  -exec grep -q '^release_status: DEPRECATED$' '{}' ';' \
  -delete

