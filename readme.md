# gaebolg

yet a simple [JSON Schema](http://json-schema.org/) extracting and flatting tool implemented by Scala 

一个简单的基于Scala实现的JSON Schema的提取和展开的工具

 Current features include:
 
 - generate JSON schema from a set of JSON instances
 - from JSON schema instance to `String` and vice versa
 - flat a JSON schema and transform it into a set of `ExecutableTableNode`

目前的功能:

- 从一组JSON实例中生成一个JSON schema
- 字符串与JSON schema的互转
- 将一个JSON schema 转成一组`ExecutableTableNode`

First of all, really appreciate [schema-guru](https://github.com/snowplow/schema-guru), which inspiring gaebolg a lot
 
首先，非常感谢[schema-guru](https://github.com/snowplow/schema-guru)，受了很多启发

### Why I made this?
   Currently, in my company, there are lots of data stored as JSON, including but not limited to MongoDB,App logs. JSON now is popular with the whole world and more and more people become familiar with it, but there comes a so wired requirement that we have to transform json and flat it into several tables based on relational database model... Large quantities of table and lack of time, we need to find some ways to make this process more automatic instead of manually making the JSON schema, flatting the JSON data according to the schema. What's worse, for some reason, no one knows the precise JSON schema(seems terribly annoying,huh). So I made this project to help me easily generate and flat json schema and hope to make json data flatting automatically.

### QuickStart
//todo 

### Use Case
//todo

### Guide & Explanation