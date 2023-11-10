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
  locales: locales.map((l) => ({
    ...l,
    path: `${docDir}/${l}`,
    repoUrl:
      l.id === "en-us"
        ? `https://github.com/StarRocks/starrocks`
        : `https://github.com/StarRocks/docs${"." + l.id}`,
    docsPath: l.id === "en-us" ? "docs/" : "",
    branchPrefix: l.id === "en-us" ? "branch-" : "",
  })),
  versions,
  enSiderbarPath: 'common/releasenotes/en-us',
  zhSiderbarPath: 'common/releasenotes/zh-cn',
  commonSiderBars: ['release_notes','ecosystem_release', 'developers']
};
