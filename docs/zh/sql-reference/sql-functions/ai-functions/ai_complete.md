---
displayed_sidebar: docs
description: "根据提示词和可选的图片或视频生成文本。"
products: [oss, enterprise, cloud, anywhere]
---

# ai_complete

使用 chat Provider 生成文本。可以只传提示词，也可以附带一张图片或一个视频，让模型根据其内容回答。

## 语法

```sql
ai_complete(prompt[, provider[, model[, options]]])
ai_complete(prompt, file[, provider[, model[, options]]])
```

## 参数

- `prompt`：VARCHAR 表达式，描述希望模型完成的任务，可使用文本列。
- `file`：提供一张图片或一个视频的 FILE 表达式。参见 [FILE 输入支持范围](ai_functions.mdx#file-input)。
- `options`：可选常量 JSON 对象，规则见 [JSON options](ai_functions.mdx#json-options)。

`provider`、`model` 的类型及默认行为见 [公共调用规则](ai_functions.mdx#公共调用规则)。

## 返回值

以 `VARCHAR` 返回模型生成的文本。

## 示例

```sql
SELECT ai_complete('用一句话解释什么是列式存储。');

SELECT ai_complete(
    prompt => '为一家书店写一句宣传语。',
    provider => 'support_chat',
    model => 'chat-model',
    options => json_object('temperature', 0.2)
);
```

[视觉模型示例](ai_functions.mdx#vision-chat)创建 `vision_chat` 并用它描述图片。使用同一个百炼 Provider 也可以描述 URL 指向的视频。

```sql
SELECT ai_complete(
    prompt => '描述这段视频中发生了什么。',
    file => to_file(named_struct(
        'uri', 'https://help-static-aliyun-doc.aliyuncs.com/file-manage-files/zh-CN/20250107/lbcemt/new+video.mp4'
    )),
    provider => 'vision_chat'
);
```
