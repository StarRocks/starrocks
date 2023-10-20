# How to contribute documentation

Thank you very much for contributing to StarRocks documentation! Your help is important to our improvement!

Before contributing, please read this article carefully to quickly understand the tips, writing process, and documentation templates.

## Tips

1. Language: Please use at least one language, Chinese or English. The bilingual version is highly preferred.
2. Index: When you add a topic, you also need to add an index for the topic in the TOC file, for example, `[introduction](/introduction/StarRocks_intro.md)`. **The topic path must be an absolute path.** This TOC file will eventually be rendered as the side navigation bar for documentation on our official website.
3. Images: Images must first be put into the **assets** folder. When inserting images into the documentation, please use the relative path, such as `![test image](../../assets/test.png)`.
4. Links: For internal links (links to documentation on our official website), please use the relative path of the document, such as `[test md](../../sql-referencest.md)`. For external links,  the format must be `[link text](link URL)`.
5. Code blocks: You must add a language identifier for code blocks, for example, ```sql.
6. Currently, special symbols are not  supported.

## Writing Process

1. **Writing phase**: Write the topic (in Markdown) according to the following template, and add the topic's index to the TOC file if the topic is newly added.

    > - *Because the documentation is written in Markdown, we recommend that you use markdown-lint to check whether the documentation conforms to the Markdown syntax.*
    > - *When adding the topic index, please pay attention to* *its category* *in the TOC file.* *For* *example, the* ***Stream Load*** *topic* *belongs to the* ***Loading*** *chapter.*

2. **Submission phase**: Create a pull request to submit the documentation changes to our documentation repository on GitHub, [English documentation repository](https://github.com/StarRocks/docs) (for the English version) and [Chinese documentation repository](https://github.com/StarRocks/docs.zh-cn) (for the Chinese version).

3. **Review phase**:

    The review phase includes automatic checks and manual review.

    - Automatic checks: whether the submitter has signed the Contributor License Agreement (CLA) and whether the documentation conforms to the Markdown syntax.
    - Manual review: Committers will read and communicate with you about the documentation. It will be merged into StarRocks documentation repository and updated on the official website.

## Documentation template

[Functions](https://github.com/StarRocks/docs/blob/main/sql-reference/sql-functions/How_to_Write_Functions_Documentation.md)
[SQL command template](https://github.com/StarRocks/docs/blob/main/sql-reference/sql-statements/SQL_command_template.md)
