import { join, relative } from "path"
import { watch } from "chokidar"
import pkg from "fs-extra"

const { copySync, removeSync } = pkg

// these paths are relative to the docusaurus dir
// so we want the en and zh folders in the parent
// to be read, and we want those files synced to
// the docs and i18n...folders in the docusaurus dir
const sourceEnglish = "../en/"
const sourceChinese = "../zh/"
const destinationEnglish = "./docs/"
const destinationChinese = "./i18n/zh/docusaurus-plugin-content-docs/current/"

// Check if --watch flag is present in the command line arguments
const should_watch = process.argv.includes("--watch")

// Copy files on startup
copySync(sourceEnglish, destinationEnglish, { recursive: true })
copySync(sourceChinese, destinationChinese, { recursive: true })

// Watch for changes if --watch flag is present
if (should_watch) {
  const watcher = watch(sourceEnglish, {
    persistent: true,
    usePolling: true,
  })

  // Watch for all events in English source folder
  watcher.on("all", (event, filePath) => {
    console.log(`${event}: ${filePath}`)
    const relativePath = relative(sourceEnglish, filePath)
    const destinationPath = join(destinationEnglish, relativePath)
    if (event === "unlink") {
      removeSync(destinationPath)
    } else {
      copySync(filePath, destinationPath)
    }
  })
}

if (should_watch) {
  const watcher = watch(sourceChinese, {
    persistent: true,
    usePolling: true,
  })

  // Watch for all events in Chinese source folder
  watcher.on("all", (event, filePath) => {
    console.log(`${event}: ${filePath}`)
    const relativePath = relative(sourceChinese, filePath)
    const destinationPath = join(destinationChinese, relativePath)
    if (event === "unlink") {
      removeSync(destinationPath)
    } else {
      copySync(filePath, destinationPath)
    }
  })
}
