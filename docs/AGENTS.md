# AGENTS.md - StarRocks Documentation

> Guidelines for AI coding agents working with StarRocks documentation.

## Overview

StarRocks documentation is built using Docusaurus and hosted at https://docs.starrocks.io/. Documentation is available in both English and Chinese.

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
3. Add both English and Chinese versions

### For Bug Fixes

1. Create PR with `[Doc]` prefix in title
2. Sign off commit: `git commit -s`
3. Select "Doc" checkbox in PR template

### Translation

When updating English docs, update Chinese docs too (or vice versa):
- `docs/en/path/to/file.md`
- `docs/zh/path/to/file.md`

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

### Update Existing Page

1. Edit the markdown file
2. Update both language versions if needed
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
