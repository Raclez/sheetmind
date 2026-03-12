# SheetMind - 企业级 Excel MCP 服务器

<p align="center">
  <img src="https://img.shields.io/badge/Java-21-blue" alt="Java 21">
  <img src="https://img.shields.io/badge/Apache%20POI-5.2.5-green" alt="Apache POI">
  <img src="https://img.shields.io/badge/MCP%20Protocol-0.13.0-orange" alt="MCP Protocol">
  <img src="https://img.shields.io/badge/License-Apache%202.0-brightgreen" alt="License">
</p>

<p align="center">
  <strong>为 AI 智能体设计的流式 Excel 处理引擎 • 处理百万行文件如喝水般简单</strong>
</p>

---

## 为什么选择 SheetMind？

市面上有 **164+** 个 Excel MCP 项目，但 SheetMind 是**唯一**能处理**百万行级别**文件而不崩溃的企业级解决方案。

### 核心优势对比

| 特性 | SheetMind | 其他 Excel MCP |
|------|-----------|----------------|
| **百万行大文件** | ✅ 流式处理，内存 <50MB | ❌ OOM 崩溃 |
| **模糊匹配** | ✅ Levenshtein 算法 | ❌ 不支持 |
| **数据透视** | ✅ 多维聚合分析 | ❌ 不支持 |
| **公式计算** | ✅ 执行 Excel 原生公式 | ❌ 不支持 |
| **虚拟线程** | ✅ Java 21 并行查询 | ❌ 串行处理 |
| **类型推断** | ✅ 自动识别数据类型 | ❌ 手动指定 |
| **安全沙箱** | ✅ 白名单 + 表达式过滤 | ❌ 几乎无防护 |
| **联邦查询** | ✅ 跨目录多文件并行 | ❌ 仅单文件 |

### SheetMind 能做什么？

```
┌─────────────────────────────────────────────────────────────────┐
│  🚀 别人做不到的，我们做到了                                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  💾 内存安全                                                    │
│     100万行 Excel 文件，传统方案直接崩溃                         │
│     SheetMind 用流式处理，内存占用永远 <50MB                     │
│                                                                  │
│  🔍 模糊匹配                                                    │
│     客户表"张三" 匹配 交易表"张山"                              │
│     Levenshtein 距离算法，智能纠错                              │
│                                                                  │
│  ⚡ 虚拟线程                                                    │
│     Java 21 虚拟线程，百万级并发                                │
│     IO 密集型任务性能提升 10 倍+                                 │
│                                                                  │
│  📊 透视分析                                                    │
│     一键生成多维分析报告                                         │
│     类似 Excel 数据透视表，但更智能                               │
│                                                                  │
│  🔐 企业级安全                                                  │
│     路径白名单 + JEXL 沙箱 + 操作审计                            │
│     放心让 AI 处理敏感数据                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 真实场景对比

| 场景 | 其他 MCP | SheetMind |
|------|---------|-----------|
| 处理 100 万行数据 | ❌ OOM 崩溃 | ✅ 流式处理 |
| 查找"张三"但表里是"张山" | ❌ 查不到 | ✅ 模糊匹配 |
| 按城市+产品统计销售额 | ❌ 写代码 | ✅ `pivot_table` |
| 执行 `=SUM(A1:A1000)` | ❌ 不支持 | ✅ `evaluate_formula` |
| 100 个 Excel 查某人打卡记录 | ❌ 串行慢 | ✅ `federated_query` 并行 |

---

## 性能指标

| 指标 | 数值 | 说明 |
|------|------|------|
| **文件大小支持** | ≤150MB（实测） | 100万行销售数据 |
| **内存占用** | <50MB（常量） | O(1) 内存复杂度 |
| **处理速度** | ~10,000 行/秒 | 现代硬件配置 |
| **表达式性能** | ~1,000 行/秒 | 包含 JEXL 表达式过滤 |

---

## 16 个 MCP 工具

| 工具 | 功能 |
|------|------|
| `scan_directory` | 扫描目录下所有 Excel 文件 |
| `list_sheets` | 列出所有 Sheet 名称 |
| `inspect_spreadsheet` | 获取表结构和预览 |
| `smart_search_rows` | JEXL 流式检索 |
| `federated_query` | 跨目录多文件联邦查询 |
| `infer_types` | 自动推断列数据类型 |
| `evaluate_formula` | 执行 Excel 原生公式 |
| `summarize_column` | 数值列统计 |
| `aggregate_table` | 分组聚合 |
| `pivot_table` | 数据透视转换 |
| `join_tables` | 多表联查 |
| `fuzzy_match` | 模糊匹配 |
| `compare_schemas` | 跨文件 Schema 对比 |
| `update_cell` | 精准更新单元格 |
| `clean_data` | 数据清洗 |
| `sort_data` | 数据排序 |
| `export_data` | 导出为新 Excel |

---

## 快速开始

### 一键安装（推荐）

```bash
npx sheetmind-mcp
```

自动完成：
- 下载对应平台的可执行文件
- 配置所有 AI 客户端（Claude Desktop、Cursor、Claude Code、OpenCode、Codex）

### 手动安装

#### 下载 Release

前往 [GitHub Releases](https://github.com/Raclez/sheetmind/releases) 下载：

| 平台 | 文件 |
|------|------|
| Linux | `sheetmind` |
| macOS | `sheetmind-macos` |
| Windows | `sheetmind.exe` |
| 全部 | `sheetmind-mcp-*.jar` (需 JDK 21) |

#### 手动配置

编辑配置文件：
- **Claude Desktop (macOS)**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Claude Desktop (Windows)**: `%APPDATA%\Claude\claude_desktop_config.json`
- **Cursor (macOS)**: `~/Library/Application Support/Cursor/cursor_app.json`
- **Cursor (Windows)**: `%APPDATA%\Cursor\cursor_app.json`

```json
{
  "mcpServers": {
    "sheetmind": {
      "command": "/path/to/sheetmind-macos"
    }
  }
}
```

### 本地构建

```bash
git clone https://github.com/Raclez/sheetmind.git
cd sheetmind/sheetmind-mcp
mvn clean package
```

---

## 架构设计

```
AI Client (Cursor/Claude) → MCP Stdio → SheetMind → Streaming Excel (POI)
                                              ↓
                              JEXL Expression Engine
                                              ↓
                              Java 21 Virtual Threads
```

### 技术栈

| 组件 | 技术 | 版本 |
|------|------|------|
| 核心引擎 | Apache POI Streaming Reader | 5.2.5 |
| 协议层 | mcp-annotated-java-sdk | 0.13.0 |
| 表达式引擎 | Apache JEXL 3 | 3.3 |
| 并行处理 | Java 21 Virtual Threads | 21 |
| 序列化 | Jackson Databind | 2.17.1 |

---

## 使用示例

### 智能搜索

```json
{
  "filePath": "/path/to/data.xlsx",
  "query": "Price > 1000 && Status == 'Done'",
  "pagination": { "limit": 20, "offset": 0 }
}
```

### 模糊匹配

```json
{
  "sourceFile": "/path/to/customers.xlsx",
  "sourceColumn": "姓名",
  "targetFile": "/path/to/orders.xlsx",
  "targetColumn": "客户姓名",
  "threshold": 0.8
}
```

### 数据透视

```json
{
  "filePath": "/path/to/sales.xlsx",
  "rows": ["城市"],
  "columns": ["产品"],
  "values": ["销售额"],
  "aggregations": ["sum"]
}
```

---

## 生产部署

### Docker

```dockerfile
FROM openjdk:21-jdk-slim
WORKDIR /app
COPY sheetmind-mcp/target/sheetmind-mcp-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
```

---

## 许可证

Apache License 2.0

---

<p align="center">
  <strong>SheetMind</strong> - 让 AI 更智能地处理 Excel
</p>

<p align="center">
  <sub>Built with ❤️ by Raclez • Java 21 • Apache 2.0</sub>
</p>
