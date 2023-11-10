# How to contribute documentation

Thank you very much for contributing to StarRocks documentation! Your help is important to help improve the docs!

Before contributing, please read this article carefully to quickly understand the tips, writing process, and documentation templates.

## Tips

1. Language: Please use at least one language, Chinese or English. The bilingual version is highly preferred.
2. Index: When you add a topic, you also need to add an entry for the topic in the table of contents(TOC) file, for example, `[introduction](/introduction/StarRocks_intro.md)`. **The path to your topic must be a relative path from the `docs` directory.** This TOC file will eventually be rendered as the side navigation bar for documentation on our official website.
3. Images: Images must first be put into the **assets** folder. When inserting images into the documentation, please use the relative path, such as `![test image](../../assets/test.png)`.
4. Links: For internal links (links to documentation on our official website), please use the relative path of the document, such as `[test md](../../sql-reference.md)`. For external links,  the format must be `[link text](link URL)`.
5. Code blocks: You must add a language identifier for code blocks, for example, `sql`.
6. Currently, special symbols are not  supported.

## Writing Process

1. **Writing phase**: Write the topic (in Markdown) according to the following template, and add the topic's index to the TOC file if the topic is newly added.

    > - *Because the documentation is written in Markdown, we recommend that you use markdown-lint to check whether the documentation conforms to the Markdown syntax.*
    > - *When adding the topic index, please pay attention to* *its category* *in the TOC file.* *For* *example, the* ***Stream Load*** *topic* *belongs to the* ***Loading*** *chapter.*

2. **Submission phase**: Create a pull request to submit the documentation changes to our documentation repository on GitHub, English documentation is in the `docs/` folder of the [StarRocks repository](https://github.com/StarRocks/starrocks) (for the English version) and [Chinese documentation repository](https://github.com/StarRocks/docs.zh-cn) (for the Chinese version).

   > **Note**
   >
   > All commits in your PR should be signed. To sign a commit you can add the `-s` argument.  For example:
   >
   > `commit -s -m "Update the MV doc"`

3. Lists of settings

   Long lists of settings like this do not index well in search, and the reader will not find the information even when they type in the exact name of a setting:

   ```markdown
   - setting_name_foo

     Details for foo

   - setting_name_bar
     Details for bar
   ...
   ```
 
   Instead, use a section heading (e.g., `###`) for the setting name and remove the indent for the text:

   ```markdown
   ### setting_name_foo

   Details for foo

   ### setting_name_bar
   Details for bar
   ...
   ```

   |Search results with a long list:|Search results with H3 headings|
   |--------------------------------|-------------------------------|
   |![image](https://github.com/StarRocks/starrocks/assets/25182304/681580e6-820a-4a5a-8d68-65852687a0df)|![image](https://github.com/StarRocks/starrocks/assets/25182304/8623e005-d6e1-4b73-9270-8bc86a2aa680)|


  
5. **Review phase**:

    The review phase includes automatic checks and manual review.

    - Automatic checks: whether the submitter has signed the Contributor License Agreement (CLA) and whether the documentation conforms to the Markdown syntax.
    - Manual review: Committers will read and communicate with you about the documentation. It will be merged into StarRocks documentation repository and updated on the official website.

## Documentation template

- [Functions](https://github.com/StarRocks/docs/blob/main/sql-reference/sql-functions/How_to_Write_Functions_Documentation.md)
- [SQL command template](https://github.com/StarRocks/docs/blob/main/sql-reference/sql-statements/SQL_command_template.md)
- [Loading data template](https://github.com/StarRocks/starrocks/blob/main/docs/loading/Loading_data_template.md)
# Docs

## Building locally

Becuase we have several versions it takes about 30 minutes to build the docs for staging. To see the results of your work quickly you will be able to build on your local laptop.

### Disable versioning

To disable versioning open a terminal and set an environment variable:

```shell
export DISABLE_VERIONING=true
```

> Note:
>
> If you want to build everything then `unset DISABLE_VERSIONING`

### Copy files into place

I will automate this later on, but here are the commands:

> Note:
>
> This assumes that your clones of the repos are in `~/GitHub`

```shell
cd ~/GitHub/docs-site/
cp -r ~/GitHub/starrocks/docs/* docs/
cp -r ~/GitHub/docs.zh-cn/* i18n/zh/docusaurus-plugin-content-docs/current/
```


## Release notes

> Note:
>
> This section of the README is not implemented yet. I tried to build the releasenotes as described below and got close, but switching from English to Chinese for the release notes was not reliable, so I backed it out. When I have time I will work with Docusaurus RD to get it working.

The way release notes are rendered in Docusaurus and in Gatsby is different. In Gatsby the links like `../quick_start/abc.md` refer to the main branch (or maybe 3.1?) no matter which version of the docs the reader is looking at. In Docusaurus when we add a release note file to a particular version those links are looking for a doc in that version.  This means almost every link from the 3.1 release notes that we copy into the 1.19 version is going to fail.

The way Docusaurus sites deal with things that should not be versioned is add them to a separate nav. At the top of our page we will have `Documentation`, `Release Notes`, version list, language list. The release notes will always be from the main branch.

During the build process the English release notes and ecosystem release notes markdown files need to be in the `docs-site/releasenotes` dir

During the build process the Chinese release notes and ecosystem release notes markdown files need to be in the `docs-site/i18n/zh/docusaurus-plugin-content-docs-releasenotes` dir

## Editing nav

At some point I will move the files used to manage the nav into the docs repos. First I need to write a configuration that will allow the writers to quickly build out the docs and see a preview of a PR.  This will involve building only the version that is being edited, and building for both Chinese and English so that the nav and content can be verified in both places. Here are the basic steps for editing the nav, full details will follow:

1. Checkout `StarRocks/docs-site`
1. Switch to the `docusaurus` branch
1. Create a new working branch from the `docusaurus` branch
1. Edit the nav for the version that you are working on
1. Submit a PR
1. Have the PR reviewed and merged
1. Run the workflow to deploy to staging

### Simple case, removing or adding a doc

This example removes a doc, and adding a doc can be done in one of two ways:

- add a markdown file to a dir that has its nav auto-generated
- add an entry to the list of items

This example removes a doc from a list of items:

#### Checkout `StarRocks/docs-site`

Ha! I tried to do all this stuff in VS Code, but what a nightmare. My fingers know the commandline, and I just can't do this with a mouse and menu. You know how to do this already anyway.

#### Switch to the `docusaurus` branch

Right now we are working in a branch named `docusaurus`, so switch there first.

#### Create a new working branch from the `docusaurus` branch

When you create your branch to work on the PR base it off of the `docusaurus` branch, not master.

#### Edit the nav for the version that you are working on

The nav files are in [`versioned_sidebars/`](https://github.com/StarRocks/docs-site/tree/docusaurus/versioned_sidebars) (nav in Docusaurus is called **Sidebar**). If you are working on 3.1 then `versioned_sidebars/version-3.1`. This file contains both English and Chinese sidebars.

> Note on file structure:
>
> The file structure for English and Chinese should be the same, if there is a file in English that is not in Chinese then the English doc will be used for both. If there is a Chinese doc that is not in English then Docusaurus will not build. At one point the Dataphin doc was not in English yet, so I had to create a dummy file.
>
> There can be nav differences, for example when there was no Dataphin doc in English I created a dummy file and just left it out of the nav. This is easy for categories with only a few entries as we can just list all of them, but for large directories full of files I just tell Docusaurus to include all of the files in the directory, so we cannot ignore files if we do that. If you compare the TOC.md for the SQL functions in Gatsby with the sidebar for Docusaurus you will see that I do not list all of the files for the SQL functions, I only list them if there are different categories mixed together in one directory. In the future I would like to create more directories and move the files into directories to match the nav and then we can save effort.

#### Submit a PR

This PR [removes a file that should not show in nav](https://github.com/StarRocks/docs-site/pull/140/files). This is easy when we list the files individually, which is true in this case.

#### Have the PR reviewed and merged

Same as always

#### Run the workflow to deploy to staging

Running the workflows is the same as it was for Gatsby, open Actions, choose the workflow, and push the button. Right now the names of the workflows are `__Stage__Deploy_docusaurus` and `__Prod__Deploy_docusaurus`

Run the `__Stage` one and view the docs at `https://docs-stage.docusaurus.io` and then deploy to Prod if you like what you see.

### Change the name of a doc

Sometimes we have really long titles in our docs, and don't want the long thing used in the nav. Alternatively, sometimes we have docs with the title `# Rules` (see the Developers > Style Guides for two examples!). There are two choices, but for now I will only give you one as we cannot use the second choice yet until I fix another problem. 

To change the name shown in the sidebar, just edit the title of the doc:

#### Change the title and therefor the nav label

In TOC.md we specified the label to associate with every category and file. We could do that with Docusaurus, but I suggest that we use the title of the doc as the sidebar label. One of the issues in the docs-site repo is about a misnamed file in the nav. The easy fix is to change the title in the file. This [PR in StarRocks/starrocks`](https://github.com/StarRocks/starrocks/pull/34243/files#diff-70c336ebca1518c87e270411fc53419ffb44cd95792a85afd1592fafd6c57e9f) fixes the problem.

### Changing the levels of the categories

There are some errors in the nav, [issue 105](https://github.com/StarRocks/docs-site/issues/105) raises one of them. When I was writing the JSON for the Administration section I thought everything was under Administration > Management. This is what the JSON looks like:

```json
    {
      "type": "category",
      "label": "Administration",
      "link": {"type": "doc", "id": "administration/administration"},
      "items": [
        {
          "type": "category",
          "label": "Management",
          "link": {"type": "doc", "id": "cover_pages/management"},
          "items": [
            "administration/Scale_up_down",
            "administration/Backup_and_restore",
            "administration/Configuration",
            "administration/Monitor_and_Alert",
            "administration/audit_loader",
            "administration/enable_fqdn",
            "administration/timezone",
            "administration/information_schema",
            "administration/monitor_manage_big_queries",
            {
              "type": "category",
              "label": "Resource management",
              "link": {"type": "doc", "id": "cover_pages/resource_management"},
              "items": [
                "administration/resource_group",
                "administration/query_queues",
                "administration/Query_management",
                "administration/Memory_management",
                "administration/spill_to_disk",
                "administration/Load_balance",
                "administration/Replica",
                "administration/Blacklist"
              ]

            },
            "administration/Data_recovery",
            {
              "type": "category",
              "label": "User Privileges and Authentication",
              "link": {"type": "doc", "id": "administration/privilege_overview"},
              "items": [
                "administration/privilege_item",
                "administration/User_privilege",
                "administration/privilege_faq",
                "administration/Authentication"
              ]
            },
            {
              "type": "category",
              "label": "Performance Tuning",
              "link": {"type": "doc", "id": "cover_pages/performance_tuning"},
              "items": [
                "administration/Query_planning",
                "administration/query_profile",
                "administration/Profiling"
              ]
            }
          ]
        }
      ]
    },
```

I think User Priv and Performance Tuning need to be moved to the same level as Management and Data Recovery.

## Old historical info

**Ignore anything below here**

Live URL: https://danroscigno.github.io/doc/docs/introduction/StarRocks_intro

## Building with GitHub actions

There are test build and deploy to GitHub Pages jobs in `.github/workflows/`.
These pull the English docs and the Chinese docs, check out the versions,
and put the Markdown files into place for Docusaurus.

Before generating the HTML some modifications are made to the Markdown files:

- removing the TOC.md and README.md files
- replacing the StarRocks_intro pages with ones that use Docusaurus styling
- adding frontmatter to all of the Markdown to specify which sidebar (English or Chinese) is to be used
- the `docs/assets/` dir is renamed to `_assets`. This is done as Docusaurus automatically
ignores markdown files in dirs that start with an underscore. This is also why I have my `scripts`
dirname. This is where I pop markdown files that I do not want included in the docs directly.

Once we go into production the three changes above can be removed as we will:

- remove the TOC.md files as they are not used, and leave the README out of the nav
- replace the current intro pages with the new ones that work with Docusaurus
- add the frontmatter to the Markdown files in thir repos
- rename the `assets` dirs to `_assets` so we don't have to do these changes in the build

## Building local

### Node version

Docusaurus v3 requires Node 18

I use 8GB for Node, in Netlify I set the build command in the file `netlify.toml` 
and locally I use:

```shell
NODE_OPTIONS=--max_old_space_size=8192
```

### Install Docusaurus

```shell
yarn install
```

### Build script

The script `scripts/testbuild`

- pulls the Chinese and English docs for versions 3.1, 3.0, and 2.5 
- Removes the intro page while we update it to use built-in nav components
- Removes the TOC while we migrate those to JSON format
- Runs an MDX checker
- Builds the site

```shell
./scripts/testbuild`
```

Note: The dir is named `scripts` because I had some markdown files that I needed to move to a dir that Docusaurus would leave out of the nav; it does not add files to the nav from dirs that start with an underscore.

## Serve the pages locally

```shell
yarn serve
```
