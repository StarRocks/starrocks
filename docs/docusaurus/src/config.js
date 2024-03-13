let locales = require("./locales.json");
let versions = require("./versions.json");

if (process.env.DOC_LOCALE && process.env.DOC_LOCALE !== "all") {
  locales = locales.filter((l) => l.id === process.env.DOC_LOCALE);
}
if (process.env.DOC_VERSION && process.env.DOC_VERSION !== "all") {
  versions = versions.filter((v) => v.branch === process.env.DOC_VERSION);
}

const docDir = "docs";

module.exports = {
  docDir,
  repoUrl: `https://github.com/StarRocks/starrocks`,
  locales: locales.map((l) => ({
    ...l,
    path: `${docDir}/${l}`,
    docsPath: "/",
    branchPrefix: "branch-",
  })),
  versions,
  enSiderbarPath: 'common/releasenotes/en-us',
  zhSiderbarPath: 'common/releasenotes/zh-cn',
  commonSiderBars: ['release_notes','ecosystem_release', 'developers']
};
