# 🌱 Scope 3 ETL Micro-SaaS | Zero-Retention Data Engine

> **“解决顶级 ESG SaaS 无法处理的非标供应链脏数据难题。”**
> 
> *Fully Aligned with GHG Protocol 2026 Scope 3 Standard Revisions.*

## 💡 业务痛点 (The "Garbage In" Problem)
在应对全球范围三（Scope 3）温室气体核算时，企业面临的最大挑战并非计算本身，而是**极度混乱的非标原始数据**。核心企业的下沉物流供应商（Category 4/9）往往提供包含异构单位（kgs, 吨, 磅）、缺失距离的脏数据，导致主流碳核算 SaaS（如 Persefoni, Envizi）无法直接导入，被迫耗费大量咨询顾问的人工时间进行清洗。

## 🚀 解决方案与核心护城河 (Value Proposition)
本工具作为一套极轻量化的**前置清洗引擎 (ETL-as-a-Service)**，完美衔接混乱的原始底稿与标准的 SaaS 导入模板。

1. **100% 阅后即焚 (Zero Data Retention)：** 数据仅在内存中通过 Pandas 进行清洗与计算，绝不落盘，彻底消除大企业 IT 合规与数据安全顾虑。
2. **审计级合规 (Audit-Ready & Immutable)：**
   - 自动填补缺失值并按 GHG 新规打上 `Primary` 或 `Estimated` 的 **Data Tier 标签**。
   - 极速生成带有 **SHA-256 数据哈希指纹** 的不可篡改 PDF 审计日志。
3. **优雅降级机制 (Graceful Degradation)：** 遇到无法识别的极端毒点数据（如乱码单位、缺失运输方式），系统绝不崩溃，而是精准挂起 `Needs Manual Review` 标签，实现人机协同的最大化效率。

## ⚙️ 核心架构 (Architecture)
- **Frontend:** Streamlit (Minimalist UI)
- **Core Engine:** Python / Pandas / Numpy
- **Auditor:** fpdf2 (In-memory PDF generation)

## 快速试用 (Demo)
1. 点击左侧栏下载 `Sample Data` 测试靶标。
2. 上传数据，体验毫秒级清洗与自动化碳排核算闭环。