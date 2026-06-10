# AGENTS.md - StarRocks Documentation

> Guidelines for AI coding agents working with StarRocks documentation.

## Overview

StarRocks documentation is built using Docusaurus and hosted at https://docs.starrocks.io/. Documentation is available in English, Chinese, and Japanese.

Read [`handbook/index.md`](../handbook/index.md) first for repo-wide routing, then use the docs domain map in [`handbook/domains/docs-and-translation.md`](../handbook/domains/docs-and-translation.md) for handbook-vs-public-docs boundaries.

## Directory Structure

```
docs/
├── en/                    # English documentation
│   ├── administration/    # Admin guides
│   ├── benchmarking/      # Benchmark guides
│   ├── data_source/       # Data source integration
│   ├── deployment/        # Deployment guides
│   ├── developers/        # Developer guides
│   ├── faq/               # FAQs
│   ├── introduction/      # Introduction
│   ├── loading/           # Data loading
│   ├── reference/         # Reference docs (SQL, config)
│   ├── sql-reference/     # SQL reference
│   └── using_starrocks/   # User guides
├── ja/                    # Japanese documentation (mirrors en/)
├── zh/                    # Chinese documentation (mirrors en/)
├── docusaurus/            # Docusaurus build config
│   ├── docusaurus.config.js
│   ├── sidebars.js
│   └── src/
└── _assets/               # Shared images and assets
```

## Building Documentation

```bash
cd docs/docusaurus

# Install dependencies
npm install

# Start development server
npm start

# Build static site
npm run build
```

## Critical Instructions

Never violate these directives. If a task would require violating them, stop and ask the user for clarification before proceeding:

- Do not add information beyond the requested changes.
- Do not modify files other than those specified by the user.
- Do not change existing frontmatter unless explicitly instructed. Adding required frontmatter fields to a new page or a page that is missing them is allowed.
- Do not use training data for StarRocks technical facts. Verify all commands, configuration options, parameter names, and version numbers against existing documentation in the same language directory before writing them. If a technical detail cannot be confirmed from existing docs, list the unconfirmed items and ask the user how to proceed.
- When editing a page in `en/`, `zh/`, or `ja/`, update the corresponding pages in the other language directories. If a `ja/` page does not yet exist, note this in your completion summary and ask the user whether to create it.
- Do not commit or push without explicit confirmation from the user.
- When a task attempt fails, try no more than two approaches before stopping to ask for instructions.
- When stopping to ask for clarification: state the blocker, list what you have verified or attempted, and ask one specific question. Do not ask multiple questions at once.

## Definition of Done

A task is complete when:

- All requested changes have been made to the specified files only.
- Corresponding pages in all three language directories (`en/`, `zh/`, `ja/`) have been updated, or the user has been informed if a `ja/` page does not exist.
- A summary of every changed file (with path) has been provided to the user.
- The user has been asked whether they are ready to commit and push.

After presenting the completion summary, wait for explicit user instruction before making further changes.

## Markdown Conventions

### Frontmatter

Every page must open with frontmatter between `---` delimiters.

**Required:**
- `displayed_sidebar: docs` — must be present on every page.
- `description` — one-sentence plain-text summary of the page (under 160 characters). Required on every page except those with `unlisted: true`. Used by Algolia search snippets and the `llms.txt` LLM index. No markdown formatting, code spans, or links.

**Optional:**
- `sidebar_position` — integer controlling sort order within a sidebar category.
- `sidebar_label` — overrides the H1 as the sidebar link text.
- `toc_max_heading_level` — integer (2–6) capping right-side TOC depth.
- `keywords` — list of terms for search and SEO.
- `unlisted: true` — keeps the URL live but hides the page from navigation and search.
- `hide_table_of_contents: true` — removes the right-side TOC.
- `title` — overrides the H1 as the browser tab title and SEO metadata.

### Language versions

All three language directories must stay in sync: `en/`, `zh/`, and `ja/`. When editing a page in one language, update the corresponding page in the other two. If a `ja/` page does not yet exist, note this in your completion summary and ask whether to create it.

### New pages

1. Create the file under `en/`.
2. Register it in `docusaurus/sidebars.js`.
3. Create matching files under `zh/` and `ja/`.

### Code blocks

Always specify a language for fenced code blocks. Never use an unlabeled fence for code that has an identifiable language.

### Admonitions

Use Docusaurus syntax — not RST-style directives:

```markdown
:::note
Supplemental information.
:::

:::tip
Helpful shortcuts or alternatives.
:::

:::warning
Risk of data loss or service disruption.
:::

:::caution
Situations requiring extra care.
:::
```

Do not stack multiple admonitions directly after one another.

### Links

Use relative paths for internal links: `[text](../category/page.md)`. Do not use absolute URLs for pages within the docs site.

### Images

Place images in `_assets/` and reference with a relative path: `![alt](../_assets/image.png)`.

## Writing Documentation

### File Format

- Use Markdown (`.md`) files
- Include frontmatter for metadata:

```markdown
---
displayed_sidebar: docs
keywords: ['keyword1', 'keyword2']
---

# Page Title

Content here...
```

### Style Guidelines

1. **Clear and concise**: Use simple language
2. **Code examples**: Include runnable examples
3. **Screenshots**: Use sparingly, keep updated
4. **Links**: Use relative links within docs

### Code Blocks

Use fenced code blocks with language:

~~~markdown
```sql
SELECT * FROM table WHERE id = 1;
```

```bash
./build.sh --fe --be
```

```java
public class Example {
    public static void main(String[] args) { }
}
```
~~~

### Images

Place images in `_assets/` and reference:

```markdown
![Description](../_assets/image-name.png)
```

## Contributing Documentation

### For New Features

1. Create doc in appropriate directory
2. Add to sidebar in `docusaurus/sidebars.js`
3. Add English and Chinese versions; add Japanese version when applicable

### For Bug Fixes

1. Create PR with `[Doc]` prefix in title
2. Sign off commit: `git commit -s`
3. Select "Doc" checkbox in PR template

### Translation

When updating English docs, update Chinese docs too (or vice versa). Update Japanese docs when a corresponding `docs/ja/` page exists or when explicitly requested:
- `docs/en/path/to/file.md` (required)
- `docs/zh/path/to/file.md` (required)
- `docs/ja/path/to/file.md` (when applicable)

## Sidebar Configuration

Edit `docs/docusaurus/sidebars.js`:

```javascript
module.exports = {
  docs: [
    {
      type: 'category',
      label: 'Category Name',
      items: [
        'path/to/doc1',
        'path/to/doc2',
      ],
    },
  ],
};
```

## Common Tasks

### Add New Page

1. Create `docs/en/category/new-page.md`
2. Add to `sidebars.js`
3. Create Chinese version `docs/zh/category/new-page.md`
4. Create Japanese version `docs/ja/category/new-page.md` (when applicable)

### Update Existing Page

1. Edit the markdown file
2. Update all language versions if needed
3. Verify links still work

### Add Code Example

```markdown
## Example

Here's how to create a table:

```sql
CREATE TABLE example (
    id INT,
    name STRING
)
DISTRIBUTED BY HASH(id);
```
```

## Preview Changes

```bash
cd docs/docusaurus
npm start
# Open http://localhost:3000
```

## Developer Documentation

Developer-specific docs are in `docs/en/developers/`:

- `code-style-guides/` - Coding standards
- `development-environment/` - Setup guides
- `build-starrocks/` - Build instructions

## Best Practices

1. **Keep it updated**: Update docs when code changes
2. **Test examples**: Verify code examples work
3. **Use templates**: Follow existing page structure
4. **Cross-reference**: Link related topics
5. **Version awareness**: Note version-specific features

## License

Documentation is under Apache 2.0 license.
