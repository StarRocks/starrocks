---
name: markdown-conventions
description: Markdown and Docusaurus formatting conventions for StarRocks docs
globs: ["en/**/*.md", "zh/**/*.md", "ja/**/*.md", "en/**/*.mdx", "zh/**/*.mdx", "ja/**/*.mdx"]
---

# Markdown Conventions

## Frontmatter

Every page must include frontmatter between `---` delimiters at the top of the file.

### Required fields

- `displayed_sidebar`: Always set to `docs`. Required on every page.
- `description`: One-sentence summary of the page content (under 160 characters). Required on every page except those with `unlisted: true`. Used by Algolia search result snippets and LLM index files (`llms.txt`). Must be plain text — no markdown formatting, no code spans, no links.

### Optional fields

| Field | Purpose |
|-------|---------|
| `sidebar_position` | Integer controlling sort order within a sidebar category |
| `sidebar_label` | Overrides the H1 heading as the text shown in the sidebar |
| `toc_max_heading_level` | Integer (2–6) capping the depth of the right-side table of contents |
| `keywords` | List of terms for search and SEO |
| `unlisted` | Set to `true` to keep the URL accessible but hide the page from navigation and search |
| `hide_table_of_contents` | Set to `true` to remove the right-side TOC from the page |
| `title` | Overrides the H1 as the page title in the browser tab and SEO metadata |

Minimal frontmatter example:

```markdown
---
displayed_sidebar: docs
description: "One sentence describing what this page covers."
---
```

## New Pages

When creating a new page:

1. Create the file in the appropriate directory under `en/`.
2. Add an entry to `docusaurus/sidebars.js` in the correct category.
3. Create matching files under `zh/` and `ja/`. If a `ja/` page does not yet exist for that section, note it in your completion summary and ask the user whether to create it.

## Language Versions

All three language directories (`en/`, `zh/`, `ja/`) must be kept in sync. When editing a page in one language directory, update the corresponding pages in the other two. Use the same relative path: `en/category/page.md` corresponds to `zh/category/page.md` and `ja/category/page.md`.

## Code Blocks

Always specify a language for fenced code blocks:

```sql
SELECT * FROM table WHERE id = 1;
```

Do not use unlabeled fences for any code that has an identifiable language.

## Admonitions

Use Docusaurus admonition syntax:

```markdown
:::note
Supplemental information that applies in specific cases.
:::

:::tip
Helpful shortcuts or alternative approaches.
:::

:::warning
Situations that could cause data loss or service disruption.
:::

:::caution
Situations requiring extra care to avoid problems.
:::
```

Do not use RST-style directives (`.. note::`) — they are not valid in Docusaurus Markdown.

Do not stack multiple admonitions directly after one another. Combine related callouts of the same type into a single admonition with separate paragraphs or a list.

## Cross-References

Use relative paths for links between pages within the docs:

```markdown
[Link text](../category/page.md)
```

Do not use absolute URLs for internal documentation pages.

## Images

Place images in `_assets/` and reference them with a relative path:

```markdown
![Alt text](../_assets/image-name.png)
```
