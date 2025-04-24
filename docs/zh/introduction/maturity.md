# Beta 和实验性功能

StarRocks 功能有五个潜在的成熟度级别：

- 实验性
- Beta
- 一般可用 (GA)
- 已弃用
- 已移除

大多数 StarRocks 功能都是 GA，如果文档中没有标签指示某个功能是实验性、Beta 或已弃用，那么该功能就是 GA。

## 实验性功能

- **稳定性**：可能存在漏洞，有一些已知的小问题。
- **成熟度**：低
- **接口**：接口可能在未来发生变化。这包括命令语法、配置参数、默认值、功能移除等。
- **可用性**：实验性功能默认关闭，需要通过 SQL 或配置文件设置参数来启用。
- **生产就绪度**：实验性功能不应在生产环境中使用。
- **支持**：请在 [GitHub issue](https://github.com/StarRocks/starrocks/issues) 上提交问题或在 [Slack](https://starrocks.io/redirecting-to-slack) 上提问，StarRocks 工程团队会尽力帮助您。

## Beta 功能

- **稳定性**：经过充分测试。
可能不适用于极端情况。
- **成熟度**：核心功能已完成，可能未进行性能优化。
- **接口**：接口可能在未来发生变化。可能不向后兼容。
- **可用性**：Beta 功能默认关闭，需要通过 SQL 或配置文件设置参数来启用。
- **生产就绪度**：不建议在生产环境中使用 Beta 功能。
- **支持**：请在 [GitHub issue](https://github.com/StarRocks/starrocks/issues) 上提交问题或在 [Slack](https://starrocks.io/redirecting-to-slack) 上提问，StarRocks 工程团队会尽力帮助您。

## GA 功能

- **稳定性**：经过全面测试。
- **成熟度**：高。
- **接口**：稳定的 API。
- **可用性**：GA 功能默认开启。
- **生产就绪度**：已准备好用于生产。
- **支持**：支持团队为客户提供支持。开源社区成员应在 [GitHub issue](https://github.com/StarRocks/starrocks/issues) 上提交问题或在 [Slack](https://starrocks.io/redirecting-to-slack) 上提问，StarRocks 工程团队会尽力帮助您。

## 已弃用功能

某些功能已被弃用——标记为移除，因为它们被其他功能替代或未被使用。通常，当某个功能被弃用时，我们会在文档中建议替代方案。