const args = process.argv;
const path = require("path");
const config = require("./src/config");
const fse = require("fs-extra");
const fs = require("fs");
const {zhSiderbarPath,enSiderbarPath,locales,commonSiderBars} = config
const versions = config.versions.filter((i) => i.branch !== "latest");
const exec = require("child_process").exec;
const execSync = require("child_process").execSync;

const tempDir = path.join(__dirname, "temp");
const docsDir = path.join(__dirname, config.docDir);
const cloneDocs = () => {
  if (fs.existsSync(tempDir)) {
    fs.rmSync(tempDir, { recursive: true });
  }
  if (fs.existsSync(docsDir)) {
    fs.rmSync(docsDir, { recursive: true });
  }
  fs.mkdirSync(docsDir);
  fs.mkdirSync(tempDir);
  Promise.all(
    locales.map((l) => {
      const tempLocaleDir = `${tempDir}/${l.id}`;
      console.log("Cloning docs:" + l.repoUrl);
      let repoUrl = l.repoUrl;
      if (process.env.DOCS_GITHUB_USER) {
        repoUrl =
          repoUrl.slice(0, 8) +
          `${process.env.DOCS_GITHUB_USER}:${process.env.DOCS_GITHUB_TOKEN}@` +
          repoUrl.slice(8);
      }
      return new Promise((resolve, reject) => {
        exec(
          `git clone ${repoUrl} ${tempLocaleDir}`,
          { env: process.env },
          () => {
            resolve();
          }
        );
      });
    })
  ).then(() => {
    copyDocs();
  });
};
const deleteDirIfExist = (path) => {
  if(fs.existsSync(path)) {
    fs.rmSync(path, { recursive: true });
  }
}
const copyDocs = () => {
  const deleteExistDir = () => {
    const enDir = path.join(__dirname, "versioned_docs");
    const zhDir = path.join(
      __dirname,
      "i18n/zh/docusaurus-plugin-content-docs/"
    );
    if (fs.existsSync(enDir)) {
      fs.rmSync(enDir, { recursive: true });
    }
    const zhFolders = fs.readdirSync(zhDir);
    zhFolders.forEach((folder) => {
      if (folder.startsWith("version-")) {
        fs.rmSync(zhDir + "/" + folder, { recursive: true });
      }
    });
  };
  deleteExistDir();
  locales.forEach((l) => {
    const tempLocaleDir = `${tempDir}/${l.id}`;
    const normalFrom = path.join(tempLocaleDir, ("/" + l.docsPath).slice(0, -1));
    let from = normalFrom
    versions.forEach((v) => {
      const targetBranch =
        v.branch === "main" ? "main" : l.branchPrefix + v.branch;
      let to = path.join(__dirname, `versioned_docs/version-${v.branch}`);
      if (l.id === "zh-cn") {
        to = path.join(
          __dirname,
          `i18n/zh/docusaurus-plugin-content-docs/version-${v.branch}`
        );
      }
      execSync(`cd ${from} && git checkout ${targetBranch}`);
      if(targetBranch === 'main'){
        const floderPath = {
          'en-us':enSiderbarPath,
          'zh-cn':zhSiderbarPath,
        }[l.id] || '/'
        to = path.join(__dirname,floderPath)
        deleteDirIfExist(to)
        commonSiderBars.forEach(floder=>{
          from = path.join(tempLocaleDir, ("/" + l.docsPath).slice(0, -1)+'/'+floder);
          fse.copySync(from, to);
          from = normalFrom
        })
      }else {
        fse.copySync(from, to);
      }
    });
  });
  execSync(`./scripts/clean_up.sh`);
  console.log("done");
};

if (args[2] === "clone") {
  cloneDocs();
}

if (args[2] === "copy") {
  copyDocs();
}
