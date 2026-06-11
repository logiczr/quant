# 多源数据架构设计

## 一、需求背景

当前系统仅使用 BaoStock 单一数据源，所有数据表硬编码为 BaoStock 的字段结构。随着系统扩展，需要引入多个数据源（如 Tushare、AKShare、Ashare 等），要求：

- 源数据原样存储，不做任何修改，确保可追溯
- 表命名体现数据源、数据频率和层级
- 业务层屏蔽底层数据源差异

## 二、三层架构设计

参考业界数据仓库分层实践，采用 **Raw → Clean → Canonical** 三层架构：

```
┌──────────────────────────────────────────────────────────────┐
│              Canonical 层（统一查询层）                        │
│  daily_bar / minute_bar / stock_info / dim_security / ...    │
│  - 标准化 code（sh.600519 格式）                               │
│  - 标准化列名（snake_case）                                    │
│  - 多源合并，去重，冲突裁决                                     │
│  - 策略/指标/前端只读这层                                      │
└──────────────────────┬───────────────────────────────────────┘
                       │ 多源合并 + 冲突裁决
┌──────────────────────┴───────────────────────────────────────┐
│              Clean 层（单源清洗层）                             │
│  clean_baostock_day   clean_tushare_day   clean_akshare_day  │
│  clean_baostock_info  clean_tushare_financial                │
│  - 字段映射（原始列名 → 标准列名）                              │
│  - 类型转换（string→date, string→float 等）                    │
│  - code 映射（原始 code → standard_code）                      │
│  - 去重、空值处理                                              │
│  - 保持数据来源单一性，便于问题定位                              │
└──────────────────────┬───────────────────────────────────────┘
                       │ 字段映射 + 类型转换 + code 映射
┌──────────────────────┴───────────────────────────────────────┐
│              Raw 层（原始落盘层）                               │
│  raw_baostock_day     raw_tushare_day     raw_akshare_day    │
│  raw_baostock_5min    raw_tushare_5min                       │
│  raw_baostock_stock_basic  raw_tushare_stock_basic            │
│  - 各源原样存储，保留原始返回字段                                │
│  - 保留原始 code 和列名                                        │
│  - 只写不改，确保数据可追溯                                     │
└──────────────────────────────────────────────────────────────┘
```

### 各层职责

| 层级 | 职责 | 输入 | 输出 | 写入者 |
|------|------|------|------|--------|
| **Raw** | 原始落盘，零处理 | API 返回的 DataFrame | 原样写入 | data_tools（各源采集模块） |
| **Clean** | 单源清洗 | Raw 层数据 | 标准化字段 + standard_code | 清洗脚本/ETL |
| **Canonical** | 多源合并 | Clean 层数据 | 统一业务视图 | 合并脚本/ETL |

### 为什么是三层而不是两层？

两层方案（Raw → Canonical）的问题：

1. **code 映射和列名标准化混在一起**：如果清洗逻辑和合并逻辑耦合，某个源的数据异常时很难定位是清洗问题还是合并问题
2. **无法单源验证**：清洗后的数据仍然按源隔离，可以单独验证 BaoStock 的清洗结果是否正确，再去做多源合并
3. **增量 ETL 更简单**：Raw → Clean 是单源的，增量逻辑清晰（按源+时间范围）；Clean → Canonical 是多源合并，逻辑更复杂。拆开后可以独立调度

实际代价：多一层存储和 ETL 管道。对于当前项目规模，Clean 层可能看起来"太重了"，但如果后续数据源超过 2 个，三层架构的维护优势会很明显。

## 三、表命名规范

### 通用格式

```
{层级前缀}_{数据源}_{数据主题}
```

### 命名示例

| 层级 | 数据源 | 数据主题 | 表名 |
|------|--------|----------|------|
| Raw | baostock | 日线行情 | `raw_baostock_day` |
| Raw | baostock | 5分钟线 | `raw_baostock_5min` |
| Raw | baostock | 股票基础信息 | `raw_baostock_stock_basic` |
| Raw | tushare | 日线行情 | `raw_tushare_day` |
| Raw | tushare | 财务数据 | `raw_tushare_financial` |
| Raw | akshare | 日线行情 | `raw_akshare_day` |
| Clean | baostock | 日线行情 | `clean_baostock_day` |
| Clean | tushare | 日线行情 | `clean_tushare_day` |
| Canonical | — | 日线行情 | `daily_bar` |
| Canonical | — | 分钟行情 | `minute_bar` |
| Canonical | — | 股票信息 | `stock_info` |

### 设计要点

1. **Raw/Clean 层用数据源名作第二段**：保持来源可追溯，一个表只有一个数据源
2. **Canonical 层不带数据源前缀**：这是跨源合并的结果，业务层不应关心数据来自哪里
3. **数据主题命名统一**：不管哪个源，"日线行情"都叫 `day`，便于 Raw → Clean 同名对应
4. **Canonical 层复用现有表名**：`daily_bar`、`minute_bar`、`stock_info` 保持不变，现有业务代码零改动

### 关于"数据主题"的约定

行情类用频率：`day`、`5min`、`15min`、`1min`
非行情类用主题名：`stock_basic`、`trade_calendar`、`financial`、`index_basic`

## 四、核心问题与解决方案

### 1. 股票代码格式统一

不同数据源使用不同的代码格式：

| 数据源 | 格式 | 示例 |
|--------|------|------|
| BaoStock | `sh.600519` | 交换所前缀.代码 |
| Tushare | `600519.SH` | 代码.交换所后缀 |
| AKShare | `600519` | 纯代码 |
| Ashare | `sh600519` | 交换所前缀+代码 |

**解决方案**：

- Raw 层：保留原始 code 列，原样存储
- Clean 层：新增 `standard_code` 列（统一为 `sh.600519` 格式），同时保留原始 code 列用于溯源
- Canonical 层：主键使用 `standard_code`

```sql
-- Raw 层
raw_baostock_day:   code = 'sh.600519'   -- BaoStock 原始格式
raw_tushare_day:    ts_code = '600519.SH' -- Tushare 原始格式

-- Clean 层
clean_baostock_day: code = 'sh.600519', standard_code = 'sh.600519'
clean_tushare_day:  ts_code = '600519.SH', standard_code = 'sh.600519'

-- Canonical 层
daily_bar: code = 'sh.600519'  -- 直接用 standard_code 作为主键
```

Code 映射规则（确定性，不需要维护映射表）：
- `600519.SH` → `sh.600519`（取前6位，SH→sh. 前缀）
- `600519` → 查 exchange 字段补全 → `sh.600519`
- `sh600519` → `sh.600519`（插入点号）

### 2. 列名标准化

不同数据源对同一字段使用不同名称：

| 语义 | BaoStock | Tushare | AKShare | 标准名 |
|------|----------|---------|---------|--------|
| 收盘价 | close | close | 收盘 | close |
| 涨跌幅 | pctChg | pct_chg | 涨跌幅 | pct_chg |
| 换手率 | turn | turnover_rate | 换手率 | turn |
| 成交额 | amount | amount | 成交额 | amount |
| 流通市值 | — | circ_mv | — | circ_mv |
| 是否ST | isST | is_st | — | is_st |

**解决方案**：
- Raw 层：保留原始列名
- Clean 层：重命名为标准列名（与当前 `daily_bar` 的 snake_case 规范一致）
- Clean 层同时保留原始列名（加 `_orig` 后缀），用于溯源和调试

### 3. 复权处理差异

| 数据源 | 复权方式 |
|--------|----------|
| BaoStock | 通过参数指定 adjustflag，API 直接返回复权后数据 |
| Tushare | 返回不复权数据 + 复权因子表（`adj_factor`），客户端自行计算 |
| AKShare | 可选前复权/后复权参数 |

**解决方案**：
- Raw 层：按各源原样存储。BaoStock 的 Raw 表带 `adjustflag` 字段；Tushare 的 Raw 表单独存 `adj_factor`
- Clean 层：统一为不复权数据 + 复权因子两套。BaoStock 需要用不复权数据重新拉取（或从复权数据反算）；Tushare 直接存原始数据 + adj_factor
- Canonical 层：`daily_bar` 存不复权数据，复权因子单独存 `adj_factor` 表，复权计算在查询时动态完成

这改变了当前 `daily_bar` 按 adjustflag 分行存储的设计，但更合理——避免同一交易日同一股票存 3 行数据。

### 4. 多源数据冲突裁决

当多个数据源覆盖同一只股票的同频数据时：

**裁决策略**：
1. 主数据源优先：为每个数据主题指定主数据源（如日线默认 BaoStock，财务数据默认 Tushare）
2. 缺口填补：主数据源缺失时，从备选数据源补全
3. 冲突标记：同一字段多源都有值但不同时，取主数据源，并在 `_source` 列记录实际来源

```sql
-- Canonical 层增加溯源列
daily_bar:
  code, date, open, high, low, close, volume, amount,
  _source = 'baostock'   -- 该行数据实际来源
```

### 5. 非行情数据的处理

`stock_info`、`trade_calendar` 等没有频率概念的数据：

- Raw 层：`raw_baostock_stock_basic`、`raw_tushare_stock_basic`
- Clean 层：`clean_baostock_stock_basic`、`clean_tushare_stock_basic`
- Canonical 层：`stock_info`（合并多源，取字段最全的源为主，其他源补字段）

### 6. DuckDB Schema 命名空间（可选增强）

DuckDB 支持 Schema 命名空间，可以替代下划线前缀：

```sql
-- 扁平命名（当前方案）
raw_baostock_day
clean_baostock_day

-- Schema 命名空间（可选）
raw.baostock_day
clean.baostock_day
canonical.daily_bar   -- 等同于 public.daily_bar
```

当前建议先用扁平命名，理由：
- 现有代码 SQL 都没有 schema 前缀，迁移成本高
- 表名前缀已足够区分层级，扁平命名更直观
- 后续如果表数量爆炸再迁移到 Schema 命名空间

## 五、与现有系统的兼容策略

### 迁移路径

**阶段一：Raw 层落地（增量，不影响现有）**
1. 新增 Raw 层表（`raw_baostock_*`）
2. 现有 `data_tools.py` → `duckdb_tools.py` 的写入路径不变
3. 在写入现有表的同时，额外写入 Raw 表
4. 现有业务代码零改动

**阶段二：Clean 层 + ETL 管道**
1. 构建 Raw → Clean 的清洗逻辑
2. 单独验证各源清洗结果的正确性

**阶段三：Canonical 层替换**
1. 构建 Clean → Canonical 的合并逻辑
2. 将现有 `daily_bar` 等表迁移为 Canonical 层
3. 业务代码（指标、策略、前端）切换到读 Canonical 层
4. 由于 Canonical 层复用现有表名和 schema，切换对业务代码透明

### 现有表的处理

| 现有表 | 迁移后角色 |
|--------|-----------|
| `stock_info` | → Canonical 层 `stock_info` |
| `daily_bar` | → Canonical 层 `daily_bar`（schema 可能微调，如去掉 adjustflag 主键、增加 _source 列） |
| `minute_bar` | → Canonical 层 `minute_bar` |
| `index_daily_bar` | → Canonical 层 `index_daily_bar` |
| `trade_calendar` | → Canonical 层 `trade_calendar` |
| `strategy_*` | 不变，策略层不参与数据分层 |
| `_strategy_meta` | 不变 |

## 六、ETL 管道设计

### 1. 触发方式

当前系统有两种数据获取模式：
- **定时拉取**：daemon 的 APScheduler 定时任务（08:30/17:40/18:10/18:30）
- **懒拉取**：`duckdb_tools.py` 的 transparent lazy pull，查询时发现缺数据自动补

引入三层架构后，ETL 链路变长，触发方式需要重新设计。

**问题：懒拉取还能存在吗？**

当前懒拉取的链路是：用户查询 → 发现缺数据 → 调 BaoStock → 写入 daily_bar → 返回。

三层架构下变成：用户查询 → 发现 Canonical 层缺数据 → 需要从 Raw 拉取 → 再跑 Clean ETL → 再跑 Canonical ETL → 返回。这个链路太长了，用户等不起。

**选项 A：干掉懒拉取，全靠定时 ETL**
- 优点：架构简单，数据层与查询层彻底解耦
- 缺点：数据新鲜度依赖 ETL 调度频率，可能出现用户要的数据还没跑完
- 适合：对实时性要求不高的场景

**选项 B：懒拉取只触发 Raw 层，Clean/Canonical 靠定时**
- 用户查询 → 触发 Raw 层拉取 → 返回"数据正在准备"
- 后台定时 ETL 把 Raw 刷到 Clean → Canonical
- 优点：Raw 层数据不缺，Canonical 层可能有延迟
- 缺点：用户体验断裂——数据已经拉了但查不到

**选项 C：懒拉取触发完整 ETL 链路，但缓存结果**
- 首次查询时同步跑完 Raw → Clean → Canonical
- 后续查询直接命中 Canonical
- 优点：用户永远能拿到数据
- 缺点：首次查询延迟高（3 层 ETL + 多个数据源拉取），实现复杂

**需要你决定的**：懒拉取还要不要保留？如果要，接受多大的查询延迟？

### 2. 增量策略

不同数据类型需要不同的增量策略：

| 数据类型 | 更新特征 | 适合策略 |
|----------|----------|----------|
| 日线行情 | 每日追加，历史不回改 | 增量追加（按日期） |
| 分钟行情 | 每日追加，历史不回改 | 增量追加（按日期） |
| 股票信息 | 不定期变更（改名/退市/ST） | 全量刷新 |
| 交易日历 | 年度追加 | 增量追加（按年） |
| 财务数据 | 季度发布，但可能修订 | 增量追加 + 修订覆盖 |
| 复权因子 | 除权除息日回改历史 | **需要全量重算** |

**问题：复权因子是最大的坑**

BaoStock 当前的做法是按 adjustflag 拉取复权后数据，每次除权除息后，所有历史数据的复权价都会变。如果 Canonical 层改为存不复权数据 + 复权因子，那么：

- 复权因子变更后，所有依赖复权价的指标（MACD、均线等）都需要重算
- 历史策略结果（`strategy_*` 表）是否需要重算？
- 如果不复算，策略回测和实盘可能用不同基准的复权数据

**问题：BaoStock 历史数据会被偷偷修正**

BaoStock 偶尔会修正历史数据（比如修正除权除息信息、补全缺失交易日）。这意味着 Raw 层"只追加不修改"的原则可能被打破——同一行数据的值变了。

你需要决定：
- Raw 层允许 UPSERT（覆盖修正）还是只 INSERT（保留旧值）？
- 如果允许覆盖，如何追溯"原始值被改过"？
- 如果不允许覆盖，Raw 层数据和 BaoStock 实际数据会逐渐偏离

**问题：增量 ETL 的边界判定**

增量 ETL 需要一个"上次跑到哪"的标记。当前用 `_strategy_meta.last_date` 做策略缓存。三层架构下需要 3 × N 个标记（3 层 × N 个表）。

- 如果用 `last_date`：某天 ETL 失败后，下次从 last_date+1 开始，但失败那天的数据已经在 Raw 层写入了一部分，Clean/Canonical 层缺了，导致数据断层
- 如果用状态机（每层每表：pending → running → done）：更精确，但实现复杂

### 3. ETL 执行顺序与依赖

三层之间有严格的依赖顺序：

```
Raw(baostock_day) ──→ Clean(baostock_day) ──→ Canonical(daily_bar)
Raw(tushare_day)  ──→ Clean(tushare_day)  ──↗
```

**问题：部分源失败怎么办？**

假设日终 ETL 流程：
1. Raw(baostock_day) ✅ 成功
2. Raw(tushare_day) ❌ 失败（API 限频/网络超时）
3. Clean(baostock_day) — 应该执行吗？
4. Clean(tushare_day) — 跳过
5. Canonical(daily_bar) — 应该合并吗？

选项：
- **等所有 Raw 完成**：一个源挂了，Canonical 层就不更新，整体延迟
- **部分推进**：BaoStock 正常跑完整个链路，Tushare 跳过，Canonical 只合并 BaoStock 的数据。等 Tushare 恢复后再补。但这意味着 Canonical 层某天的 `_source` 只有一个源，冲突裁决逻辑可能不一致
- **降级回退**：如果主数据源失败，用备源数据临时顶上

**问题：Clean 和 Canonical 的执行频率**

当前 daemon 的定时任务是一次性跑完所有股票（5000+ 只）。三层架构下：
- 如果 Raw → Clean → Canonical 同步串行执行，收盘后跑一次可能要好几个小时
- 如果异步执行（Raw 完成后触发 Clean，Clean 完成后触发 Canonical），需要消息队列或事件机制，架构变重

### 4. DuckDB 并发写入

**这是一个硬约束。**

DuckDB 只支持**单进程写入**。当前架构下 daemon 独占写入，没问题。但三层 ETL 意味着写入操作更多更频繁：

- Raw 层写入（多数据源并行拉取？）
- Clean 层写入（清洗结果）
- Canonical 层写入（合并结果）
- 策略层写入（查询触发计算）

如果 ETL 期间用户触发了懒拉取或策略计算，可能出现写入冲突。

DuckDB 的读写在同一进程中可以并发（多线程读 + 单线程写），但跨进程不行。

**可能的解决方案**：
- 所有写入操作必须通过 daemon 统一调度，不允许 Streamlit 侧直接写 DB
- 这其实和 Docker 架构一致（streamlit 容器不挂 DB volume），但当前非 Docker 部署下 `duckdb_tools.py` 的写入是散布在各模块的
- 或者：引入写入队列，daemon 消费队列执行写入

### 5. 数据质量校验

ETL 跑完不代表数据正确。需要定义校验规则：

| 校验维度 | 示例 | 严重程度 |
|----------|------|----------|
| 行数检查 | 日线数据每天应有 ~5000 行（全 A 股），如果只有 50 行说明拉取有误 | 阻断 |
| 空值率 | close 为空的行占比不应超过 0.1% | 告警 |
| 值域检查 | pct_chg 不应超过 ±20%（正常股票）/ ±44%（创业板注册制） | 告警 |
| 连续性 | 交易日不应有缺口 | 告警 |
| 跨源一致性 | BaoStock 和 Tushare 的同一股票同日收盘价差异不应超过 0.1% | 告警 |

**问题：校验失败怎么办？**

- 阻断下游 ETL？（坏数据不进入 Clean/Canonical，但这意味着数据缺失）
- 标记并放行？（数据可用但带质量标签，业务层需要感知）
- 丢弃异常行？（可能导致某只股票某天缺失，指标计算断档）

### 6. 回填与历史数据初始化

首次引入新数据源时，需要回填历史数据。问题是：

**回填量巨大**：
- 5000 只股票 × 10 年日线 ≈ 1200 万行
- Tushare 有积分限制，每次请求最多 5000 行，需要 2400+ 次请求
- BaoStock 无硬性限制但会限频，大范围拉取可能被临时封禁

**回填期间的 ETL 状态**：
- 回填可能跑几个小时甚至几天
- 期间正常的日终 ETL 还要不要跑？
- 回填到一半的数据（比如历史日线拉到 2020 年，但 2024 年的还没拉）在 Clean/Canonical 层如何处理？

**问题：分钟数据的回填限制**

BaoStock 分钟数据只保留最近 3 个月。如果现在才建 Raw 层，历史分钟数据永远回填不了。其他源的分钟数据历史覆盖也不一定完整。这意味着 Clean/Canonical 层的分钟数据可能只从"引入多源架构的那天"才开始有。

### 7. 数据修正（Correction）处理

现实场景：
- 某股票除权除息信息被 BaoStock 修正，导致历史复权因子变化
- 某股票因停牌原因，之前的日线数据需要删除
- Tushare 修正了某只股票的财务数据

Raw 层"只追加不修改"的原则 vs 数据源主动修正了历史数据，怎么处理？

**选项 A：Raw 层允许 UPSERT，覆盖修正**
- 简单，Raw 层始终和源数据一致
- 但丢失了修改历史，无法回答"这个值之前是什么"

**选项 B：Raw 层追加新版本，保留历史**
- 每次写入带版本号或写入时间
- 查询时默认取最新版本，但可以回溯
- 存储成本更高，查询逻辑更复杂

**选项 C：Raw 层追加 + 维护 changelog**
- Raw 层正常 UPSERT
- 修正时额外写一条到 `raw_changelog`（表名、主键、旧值、新值、时间戳）
- 大部分情况不需要 changelog，只在数据被覆盖时记录

### 8. ETL 可观测性

当前系统几乎没有 ETL 监控——`last_fetch.json` 是唯一的状态记录。三层架构下 ETL 链路更长，出问题更难定位。

**最低要求**：
- 每个 ETL 任务的执行状态（成功/失败/运行中）
- 每层每表的行数统计和时间戳
- 失败时的错误信息和重试次数

**进阶要求**：
- ETL 执行耗时统计
- 数据新鲜度指标（Canonical 层最新数据是哪个时间点的）
- 异常告警（钉钉/企业微信推送）

### 9. 对现有懒拉取的冲击

当前 `duckdb_tools.py` 的 `get_daily()` 实现了透明懒拉取：查 DB → 缺数据 → 调 BaoStock → 写 daily_bar → 返回。

这个逻辑直接写入 Canonical 层（当前叫 `daily_bar`），跳过了 Raw 和 Clean 层。

三层架构上线后，这个逻辑必须改造。但懒拉取是当前用户体验的核心——用户选一只股票就能立即看到数据，不用等 ETL。

**需要你决定的关键问题**：三层架构下，用户触发的即时查询走哪条路径？

- 路径 1：只读 Canonical 层，数据没准备好就提示"请等待 ETL"
- 路径 2：即时走 Raw → Clean → Canonical 链路（慢但完整）
- 路径 3：即时走快捷路径（直接写 Canonical，跳过 Raw/Clean），后台定时补齐 Raw/Clean
- 路径 4：混合模式——日终 ETL 覆盖的数据走 Canonical，新数据走快捷路径

路径 3 最务实但有风险：Canonical 层存在"未经过 Raw/Clean 层验证"的数据，数据追溯链断裂。如果后续发现这批数据有问题，没有 Raw 层可以比对。

## 七、非行情数据的组织

行情数据（日线/分钟线）是规则的、高频的、结构化的。但量化系统还需要其他类型的数据，它们和行情数据的性质完全不同，直接套用三层模型会遇到问题。

### 1. 数据类型分析

| 数据类型 | 更新频率 | 数据量 | 结构性 | 典型来源 |
|----------|----------|--------|--------|----------|
| 季报/年报财务数据 | 季度，但会修订 | 中 | 高度结构化 | Tushare, BaoStock, AKShare |
| 新闻/公告 | 实时/不定 | 大 | 非结构化文本 | 东方财富, 同花顺, 新闻API |
| 研报 | 不定 | 中 | 半结构化（PDF→文本） | 券商API, 东方财富 |
| 龙虎榜 | 交易日 | 小 | 结构化 | 东方财富 |
| 大宗交易 | 交易日 | 小 | 结构化 | Tushare, AKShare |
| 融资融券 | 交易日 | 小 | 结构化 | Tushare |
| 股东变动 | 不定 | 小 | 结构化 | Tushare, BaoStock |
| 行业分类 | 年度修订 | 极小 | 结构化 | 申万, 证监会 |

### 2. 财务数据的核心问题

#### 问题一：Point-in-Time（PIT）

这是财务数据最致命的问题，也是量化回测中最容易被忽视的。

**场景**：某公司 2024-04-30 发布 2024Q1 季报，EPS = 0.5。随后 2024-08-15 修订为 EPS = 0.45。

- 如果你的策略在 2024-06-01 买入，基于的是 EPS = 0.5
- 但如果你在 2024-09-01 回测 2024-06-01 的决策，数据库里已经是修订后的 0.45
- **回测用了未来信息**，策略表现虚高

这不是理论问题——A 股财务数据修订非常常见，尤其是：

- 会计差错更正
- 审计调整
- 同一控制下企业合并追溯调整

**解决方案：PIT 表**

每条财务记录带 `report_date`（报告期）+ `announce_date`（公告日期）。查询时用 `announce_date <= 策略日期` 过滤，确保只用当时已知的信息。

```sql
-- Canonical 层 financial_data 表
code, report_date, announce_date, metric, value, source, is_revised
sh.600519, 2024-03-31, 2024-04-30, eps, 0.50, tushare, false   -- 原始发布
sh.600519, 2024-03-31, 2024-08-15, eps, 0.45, tushare, true    -- 修订
```

回测查询：
```sql
SELECT * FROM financial_data
WHERE code = 'sh.600519'
  AND announce_date <= '2024-06-01'   -- 只用当时已公告的数据
  AND is_revised = false               -- 或取修订版，取决于策略需求
```

**问题**：不是所有数据源都提供 `announce_date`。BaoStock 的财务数据就没有公告日期字段，只有报告期。Tushare 有 `ann_date`。如果用 BaoStock 的财务数据做回测，PIT 无法保证。

#### 问题二：累计值 vs 单季值

A 股财务报表的利润表和现金流量表是**累计值**（年初至报告期），不是单季度值：

- 一季报：1-3 月累计
- 半年报：1-6 月累计
- 三季报：1-9 月累计
- 年报：1-12 月累计

策略通常需要**单季度值**（比如 Q3 营收 = 三季报累计 - 半年报累计）。这个计算看似简单，实际有很多坑：

- 会计准则变更：某年营收口径变了，累计值不能直接相减
- 报告期缺失：公司没发一季报，半年报减去年年报得到的是半年数据而非 Q2
- 追溯调整：年报修订后，前面季度的单季值也要跟着重算

**结论**：Clean 层的财务数据清洗逻辑远比行情数据复杂，不能简单做列名映射。需要专门的财务数据清洗模块。

#### 问题三：财务数据不适合增量追加

行情数据天然是时间递增的（每天新增一行），但财务数据：

- 一只股票一个报告期可能有多行（不同指标）
- 修订时需要更新历史行
- 新发季报时，之前季度的单季值可能需要重算

所以财务数据的 ETL 不应该是"增量追加"，更接近"快照替换"——每次拉取时获取截至当前的所有财务数据，整体替换 Clean/Canonical 层。

### 3. 新闻数据的核心问题

#### 问题一：存储媒介选择

新闻是非结构化文本，单条可能几百到几千字。DuckDB 存文本技术上可行，但：

- 大量文本会让 DuckDB 文件迅速膨胀
- DuckDB 的文本检索能力有限（无全文索引，只能 LIKE）
- 新闻量级：A 股每天数百条公告 + 新闻，一年 ~10 万条，10 年 ~100 万条

**选项**：

| 方案 | 优点 | 缺点 |
|------|------|------|
| DuckDB 存全文 | 统一存储，查询简单 | 文件膨胀，检索慢 |
| DuckDB 存元数据 + 文本存文件 | 查询元数据快，文本可单独处理 | 两套存储，一致性难维护 |
| DuckDB 存元数据 + 向量数据库存 embedding | 支持语义搜索 | 架构更重，embedding 有损 |
| 不存原始文本，只存结构化信号 | 存储小，直接可用 | 丢失原始信息，不可追溯 |

**建议**：先想清楚新闻数据的用途——是用来做情感分析（需要全文/NLP），还是只记录事件类型（结构化就够了）？用途决定存储方式。

#### 问题二：去重

同一事件会被多个来源报道：
- 东方财富和同花顺报道同一条新闻，措辞不同
- 上市公司公告在巨潮资讯和上交所官网都有
- 转载/聚合导致的重复

Raw 层按"各源原样存储"的原则，重复是合理的。但 Clean 层需要做去重/归并，这需要：

- 去重键：公告类用公告编号，新闻类没有唯一标识，只能靠时间+标题相似度
- 相似度计算：标题完全相同？还是模糊匹配？
- 去重后保留哪一版？内容最全的？还是最早的？

#### 问题三：新闻数据没有"频率"

当前命名规范 `raw_{source}_{theme}` 中，行情类用频率作 theme（day, 5min），但新闻没有频率。它也不像 `stock_basic` 那样是静态的。

新闻是**事件驱动**的——数据产生不遵循时间规律，一天可能 0 条也可能 100 条。这和定时 ETL 的调度模型天然不匹配。

### 4. 三层模型对非行情数据的适用性评估

| 数据类型 | Raw 层 | Clean 层 | Canonical 层 | 三层模型适用？ |
|----------|--------|----------|--------------|----------------|
| 日线行情 | ✅ 原样存储 | ✅ 列名/code映射 | ✅ 多源合并 | ✅ 完全适用 |
| 财务数据 | ✅ 原样存储 | ⚠️ 需要PIT处理、累计→单季转换 | ⚠️ 快照替换而非增量合并 | ⚠️ 框架适用，Clean层逻辑远比行情复杂 |
| 新闻 | ✅ 原样存储 | ⚠️ 去重+NLP提取 | ❌ 难以定义"统一视图" | ⚠️ Raw/Clean 有用，Canonical 层意义不大 |
| 龙虎榜/大宗交易 | ✅ 原样存储 | ✅ 列名/code映射 | ✅ 单源为主，合并简单 | ✅ 适用，和行情类似 |
| 行业分类 | ✅ 原样存储 | ✅ 映射 | ⚠️ 多套分类体系并存 | ⚠️ Canonical层可能需要同时保留申万和证监会两套 |

### 5. 非行情数据的命名规范

对于没有频率的数据类型，theme 用业务领域名：

```
{layer}_{source}_{domain}_{subject}
```

| 数据类型 | Raw 表名 | Clean 表名 | Canonical 表名 |
|----------|----------|------------|----------------|
| 季报财务 | `raw_tushare_financial_income` | `clean_tushare_financial_income` | `fin_income` |
| 资产负债 | `raw_tushare_financial_balance` | `clean_tushare_financial_balance` | `fin_balance` |
| 新闻 | `raw_eastmoney_news` | `clean_eastmoney_news` | `news`（如需） |
| 龙虎榜 | `raw_eastmoney_lhb` | `clean_eastmoney_lhb` | `lhb` |
| 大宗交易 | `raw_tushare_block_trade` | `clean_tushare_block_trade` | `block_trade` |
| 申万行业 | `raw_sw_industry` | `clean_sw_industry` | `industry_sw` |
| 复权因子 | `raw_tushare_adj_factor` | `clean_tushare_adj_factor` | `adj_factor` |

注意：
- 财务数据按报表类型拆表（income/balance/cashflow），不是一张大宽表。因为利润表和资产负债表的字段完全不同，硬塞一张表会很别扭
- 新闻的 Canonical 层需要根据用途决定是否存在——如果只做情感打分，`news` 表可能只需要 `code, date, sentiment_score`，而非全文

### 6. 新闻数据是否应该纳入三层模型？

**核心矛盾**：三层模型是为结构化、可合并的行情数据设计的。新闻是非结构化的、去重逻辑复杂的、不一定需要跨源合并的。

**建议：新闻数据只走 Raw + Clean 两层**

```
Raw 层：raw_eastmoney_news, raw_eastmoney_announcement
   ↓ NLP 提取（实体识别、情感分析、事件分类）
Clean 层：clean_news_signal（code, date, event_type, sentiment, summary）
```

Clean 层产出的是**结构化信号**，而非原文。策略层直接读 Clean 层的信号表。

这样做的理由：
- 新闻不需要"多源合并成统一视图"——东方财富和同花顺的新闻不需要合并成一条
- 策略关心的是"这只股票今天有没有负面新闻"，不关心新闻来自哪个源
- 原文保留在 Raw 层供追溯，策略层不需要读原文

### 7. 需要你回答的问题

| # | 问题 | 影响 |
|---|------|------|
| 1 | 财务数据用哪个源为主？BaoStock 没有 announce_date，做不了 PIT | 决定回测可靠性 |
| 2 | 财务数据是按报表类型拆表，还是一张大宽表？ | 决定 schema 设计 |
| 3 | 新闻数据的用途是什么？情感分析 / 事件驱动 / 仅记录？ | 决定存储方式和是否需要 Canonical 层 |
| 4 | 新闻全文存哪里？DuckDB / 文件系统 / 只存摘要？ | 决定存储架构 |
| 5 | 行业分类要不要同时保留多套体系（申万/证监会/中信）？ | 决定 Canonical 层结构 |
| 6 | 财务数据的累计→单季转换在哪层做？Clean 还是 Canonical？ | 决定 Clean 层复杂度 |

### 8. 非结构化数据存储方案

先明确"非结构化数据"在这个项目里具体指什么：

| 数据 | 体量 | 文本长度 | 更新频率 | 需要全文检索？ |
|------|------|----------|----------|----------------|
| 上市公司公告 | ~500条/天 | 数百~数万字 | 盘后密集发布 | 可能需要 |
| 新闻资讯 | ~200条/天 | 数百~数千字 | 全天 | 可能需要 |
| 研报 | ~50份/天 | 数万字（PDF） | 不定 | 可能需要 |
| 管理层讨论 | 季度 | 数千字 | 季度 | 不太需要 |

年增量估算：~30万条，文本总量约 5-10GB/年。

#### 行业主流做法

数据仓库/湖仓领域对非结构化数据有成熟的处理模式，核心思路是**"原始文件 + 元数据双轨"**：

**1. Lakehouse / Medallion 架构（Databricks 主推）**

Bronze（原始层）→ Silver（清洗层）→ Gold（业务层），非结构化数据是"一等公民"：

- Bronze 层：原始文件原样落地（PDF、HTML、JSON 照存），同时提取元数据入表
- Silver 层：从非结构化内容中提取结构化信号（实体、事件、情感），与结构化数据同层存放
- Gold 层：面向业务的分析视图

关键做法：
- **原始文件永远保留**，即使已经提取了结构化信息。云存储/对象存储便宜，不删
- **元数据是桥梁**：从非结构化数据中提取"通用连接器"——时间、地点、标识符、事件、金额——作为与结构化数据 JOIN 的关联键
- 文件用对象存储（S3/OSS），元数据用数据仓库（Delta Lake / Parquet 表）

**2. 数据湖模式（传统 Hadoop 生态）**

- 原始文件存 HDFS / S3
- Hive Metastore 管理元数据
- 外部表（External Table）引用文件路径，不搬运数据
- 处理靠 Spark / Flink 等计算引擎

**3. 对象存储 + 元数据服务（中小规模主流）**

- 文件存 S3 / MinIO / OSS（或本地文件系统模拟对象存储）
- 元数据（文件路径、标题、日期、关联股票代码等）存关系型数据库
- 查询走元数据 → 按需拉文件
- 这是云原生场景最轻量的做法

**4. 向量数据库 + RAG（AI 时代新增）**

- 文件提取文本 → 切片 → 生成 embedding → 存向量数据库
- 用于语义搜索和 LLM 问答
- 通常和方案 3 组合使用，不是替代

#### 对本项目的映射

上述做法都是"大厂方案"（S3 + Spark + Delta Lake），我们用 DuckDB + 本地文件系统，但核心思路是一样的：

| 业界做法 | 我们的对应 |
|----------|-----------|
| 对象存储（S3/OSS） | 本地文件系统 `data/text/raw/` |
| 元数据表（Hive Metastore / Delta Lake） | DuckDB `raw_text_index` 表 |
| Spark ETL 提取结构化信号 | Python 脚本 + NLP/LLM |
| Silver/Gold 层 Parquet 表 | DuckDB Clean/Canonical 层 |
| 向量数据库（ChromaDB） | 未来按需引入 |

本质上就是**方案 E（分层存储）**，只是把"对象存储"换成了"本地文件系统"。如果未来部署到云上，`data/text/raw/` 可以直接映射到 S3/OSS bucket。

#### Raw 层数据的三层存储结构

很多"非结构化"文件其实是半结构化的——PDF 里的财务报表有明确的行列结构，公告里文本和表格混排，研报的核心结论在表格里。如果只存 meta.json + raw file，表格数据要么丢弃要么每次重新解析，浪费且不可靠。

**核心思路：从原始文件中提取出结构化表格数据，与元数据和原始文件并存。**

```
meta.json          → 元数据（标题、时间、关联code、来源URL、文件结构描述）
tables/            → 从原始文件中提取的结构化表格数据（CSV/Parquet）
原始文件            → PDF/HTML 原样保存
```

这三层不是"任意可选"，而是根据数据类型的结构化程度有不同组合：

| 数据类型 | meta.json | tables/ | 原始文件 | 说明 |
|----------|:---------:|:-------:|:--------:|------|
| 新闻 | ✅ 必须 | ❌ 无 | ✅ 可选 | 纯文本，无表格可提取 |
| 公告（纯文本类） | ✅ 必须 | ❌ 无 | ✅ 可选 | 如减持预披露、股东大会通知 |
| 公告（含报表类） | ✅ 必须 | ✅ 必须 | ✅ 必须 | 如回购进展、股权激励草案中的附表 |
| 定期报告（季报/年报） | ✅ 必须 | ✅ 必须 | ✅ 必须 | 核心价值在表格，必须提取 |
| 研报 | ✅ 必须 | ✅ 可选 | ✅ 可选 | 预测表和估值表值得提取 |
| 龙虎榜 | ✅ 必须 | ✅ 必须 | ❌ 无需 | 本身就是表格，原始页面无额外价值 |

#### 目录结构

```
data/text/raw/
├── eastmoney/
│   ├── news/
│   │   └── 2024/06/07/
│   │       └── 00123/
│   │           └── meta.json                # {id, code, title, content, publish_time, ...}
│   ├── announcement/
│   │   └── 2024/06/07/
│   │       ├── 00124/
│   │       │   ├── meta.json                # 元数据 + 正文摘要
│   │       │   └── 00124.pdf                # 原始公告
│   │       └── 00125/                       # 含报表的公告
│   │           ├── meta.json                # 元数据 + table_catalog（描述有哪些表）
│   │           ├── tables/
│   │           │   ├── buyback_progress.csv # 回购进展表
│   │           │   └── top10_holders.csv    # 前十股东表
│   │           └── 00125.pdf                # 原始公告
│   └── research_report/
│       └── 2024/06/07/
│           └── 00126/
│               ├── meta.json                # 元数据 + 评级、目标价
│               ├── tables/
│               │   └── earnings_forecast.csv # 盈利预测表
│               └── 00126.pdf                # 原始研报
├── cninfo/                                  # 巨潮资讯
│   └── financial_report/
│       └── 2024/Q1/
│           └── sh600519_2024Q1/
│               ├── meta.json                # {code, report_date, announce_date, ...}
│               ├── tables/
│               │   ├── income.csv           # 利润表
│               │   ├── balance.csv          # 资产负债表
│               │   └── cashflow.csv         # 现金流量表
│               └── sh600519_2024Q1.pdf      # 原始报告
└── tushare/
    └── news/
        └── ...
```

#### meta.json 结构规范

```json
{
  "id": "eastmoney_ann_00125",
  "source": "eastmoney",
  "type": "announcement",
  "subtype": "buyback_progress",
  "code": "sz.002149",
  "title": "关于回购公司股份的进展公告",
  "publish_time": "2024-06-07 18:30:00",
  "source_url": "https://...",
  "raw_file": "00125.pdf",
  "has_tables": true,
  "table_catalog": [
    {
      "name": "buyback_progress",
      "file": "tables/buyback_progress.csv",
      "description": "回购进展明细",
      "columns": ["date", "method", "volume", "amount", "avg_price"]
    }
  ],
  "content": "本公司于2024年5月1日至2024年5月31日期间...",
  "keywords": ["回购", "集中竞价"],
  "fetch_time": "2024-06-07T19:00:00"
}
```

关键字段说明：
- `has_tables`：标识是否有结构化表格，查询时快速判断是否需要读 tables/
- `table_catalog`：表格目录，描述每个表的文件路径、含义和列名。Clean 层 ETL 用它定位和校验表格数据
- `content`：提取的纯文本（可选，磁盘紧张时可以只存摘要）
- `fetch_time`：采集时间，和 `publish_time` 区分

#### tables/ 的格式选择

| 格式 | 优点 | 缺点 | 建议 |
|------|------|------|------|
| CSV | 通用，可读，任何工具都能打开 | 无类型信息，数值可能被 Excel 篡改 | 简单场景用 |
| Parquet | 有类型，压缩好，DuckDB 直读 | 二进制不可人工查看 | 大量表数据时用 |
| JSON | 灵活，嵌套结构也能存 | 冗余大，表格数据不如 CSV 直观 | 不推荐 |

**建议**：默认用 CSV，加一个同名的 `.schema.json` 描述列类型（可选）。理由：
- CSV 人工可读可编辑，调试方便
- 财务数据的表格行数不会很大（单只股票单季度就几十行），CSV 的性能劣势不存在
- Parquet 留给 Clean/Canonical 层——那层才是 DuckDB 高频查询的地方

如果某个源产出的表特别多或特别大（比如全市场财务数据一次性提取），可以升级为 Parquet。

#### 提取表格数据的时机

表格数据在 Raw 层就提取，而不是推迟到 Clean 层。理由：

1. **Raw 层的核心原则是"可追溯 + 可用"**：如果 Clean 层要从 PDF 重新解析表格，解析失败就没数据了。Raw 层提前提取，Clean 层直接读 CSV，可靠性更高
2. **解析结果可校验**：提取表格后可以和原始 PDF 人工比对，确认提取正确。如果推迟到 Clean 层，出错时需要回溯到原始 PDF
3. **解析是一次性的**：PDF 解析（尤其是表格提取）是有损的、不可重复的（不同解析工具结果不同）。在 Raw 层提取一次、固化结果，避免重复解析的不确定性

但这带来一个约束：**Raw 层的 ETL 不是纯"原样落盘"了**，而是"原样落盘 + 结构化提取"。严格来说表格提取属于 Clean 层的工作。权衡之下，建议把表格提取看作 Raw 层的**附加动作**——原始文件才是 Raw 层的主体，表格是原始文件的**结构化投影**，和 meta.json 是同一性质。

#### 和三层架构的关系

更新后的非结构化数据三层架构：

| 层级 | 存储 | 内容 |
|------|------|------|
| Raw | 文件系统（meta.json + tables/ + 原始文件） + DuckDB 索引表 | 原始数据，元数据可查，表格已提取 |
| Clean | DuckDB | 标准化信号：code映射、列名统一、PIT处理、情感分析 |
| Canonical | DuckDB（可选） | 多源合并的业务视图 |

**数据流**：

```
原始PDF/HTML
  ├─→ meta.json           → DuckDB raw_text_index（元数据索引）
  ├─→ tables/*.csv        → DuckDB clean 层（标准化清洗）
  │                         （code映射、列名统一、PIT标注等）
  └─→ 原始文件保留          → 备查 / 未来重新提取

新闻类（无表格）
  ├─→ meta.json           → DuckDB raw_text_index
  └─→ content字段         → NLP提取 → DuckDB clean 层（event_type, sentiment）
```

**关键变化**：Raw 层的表格数据可以直接进入 Clean 层的 ETL，不需要重新解析原始文件。Clean 层的工作从"解析+清洗"简化为"清洗"——只做 code 映射、列名标准化、类型转换这些确定性工作。这大大降低了 Clean 层的复杂度和失败率。

### 9. 数据库操作模型：任务队列方案

#### 当前方式的问题

```
当前：网络I/O和DB I/O交织在一起

用户查询 get_daily()
  ├─ 1. 读 DB（检查本地数据）          ← DB 读
  ├─ 2. 发现缺数据，调 BaoStock API     ← 网络I/O（可能10秒+）
  ├─ 3. 写 DB（存拉取结果）             ← DB 写
  └─ 4. 返回数据给用户

问题：
  - 步骤2期间，DB 没有被使用但逻辑上这个操作"持有"着上下文
  - 步骤2如果超时/失败，步骤3的写入状态不确定
  - 定时批量拉取（步骤2处理5000只股票）和用户懒拉取可能同时到达步骤3
  - stock_info 的全表 DELETE+INSERT 有空窗期
  - 写入散布在 duckdb_tools / strategy / bs_zone，没有统一入口
```

#### 提议方案：任务队列

**核心假设**：所有 DB 操作执行时间 < 500ms。如果超过，说明操作本身需要拆分。

**核心思路**：数据拉取/计算和 DB 读写彻底分离。拉取阶段不碰 DB，只在拉取完成后，生成一个"DB 任务"入队，由专门的 DB Worker 串行消费。

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Producer     │     │   Task Queue │     │  DB Worker   │
│  (数据拉取/   │────→│              │────→│  (单线程串行  │
│   策略计算/   │     │  FIFO 队列   │     │   执行DB操作) │
│   用户查询)   │     │              │     │              │
└──────────────┘     └──────────────┘     └──────────────┘
       ↑                                          │
       │              ┌──────────────┐             │
       └──────────────│ Task Status  │←────────────┘
                      │ (任务状态查询) │
                      └──────────────┘
```

**任务结构**：

```python
@dataclass
class DBTask:
    task_id: str          # 唯一ID（UUID）
    task_type: str        # "read" / "write" / "delete" / "create"
    table: str            # 目标表名
    status: str           # "pending" / "running" / "done" / "failed"
    data: Any             # 写入数据（DataFrame）或查询参数
    result: Any           # 执行结果（查询返回的DataFrame等）
    created_at: datetime  # 创建时间
    started_at: datetime  # 开始执行时间
    finished_at: datetime # 完成时间
    error: str | None     # 失败原因
```

**流程示例：拉取日线数据**

```
1. Producer: 调 BaoStock 拉取 sh.600519 日线 → 得到 DataFrame（耗时2-5秒）
2. Producer: 创建 DBTask(type="write", table="raw_baostock_day", data=DataFrame)
3. Producer: task_id = enqueue(task)  → 立即返回 task_id
4. DB Worker: 从队列头部取任务 → 执行 UPSERT → 标记 status="done"
5. 任何人: query_task_status(task_id) → 查看 result 或 status
```

**流程示例：用户查询日线**

```
1. Producer: 创建 DBTask(type="read", table="daily_bar", data={"code":"sh.600519", ...})
2. Producer: task_id = enqueue(task)
3. 等待 DB Worker 执行完成
4. 返回 task.result
```

#### 比原方案好在哪里

| 维度 | 原方案 | 任务队列方案 |
|------|--------|-------------|
| **写入冲突** | 多个触发源可能同时写，依赖 DuckDB 行锁 | DB Worker 单线程串行，零冲突 |
| **网络I/O占DB** | 懒拉取时网络请求和DB操作交织 | 拉取完成才入队，DB操作和I/O完全分离 |
| **stock_info空窗** | DELETE+INSERT 之间可读到空表 | 任务粒度是"替换"，DB Worker 内部可以用事务保证原子性 |
| **策略写入原子性** | 先DELETE再INSERT，中间可读到空 | 同上，事务保证 |
| **可观测性** | 几乎没有（只有 last_fetch.json） | 每个任务有状态、耗时、错误信息 |
| **写入入口** | 散布在多个模块 | 统一通过队列，所有DB操作可审计 |

#### 需要拷打的问题

**问题一：读操作要不要入队？**

你的提议是"前端查询也包装成任务"。这需要分情况：

| 场景 | 预期耗时 | 入队？ | 理由 |
|------|---------|--------|------|
| 单只股票日线查询 | <10ms | ❌ 不需要 | DuckDB 列存+索引，极快 |
| 全市场扫描（5000只） | 50-200ms | ❌ 不需要 | 仍然在500ms内 |
| 策略 compute（含读取+计算） | 可能>500ms | ⚠️ 看情况 | 耗时在计算而非DB读 |
| 批量写入（5000只日线） | 可能>500ms | ✅ 需要 | 但应该拆分成多个小任务 |

DuckDB 支持**多线程并发读 + 单线程写**。读任务之间不会冲突，读和写也可以并发（同一进程内）。如果读也入队串行执行，反而浪费了 DuckDB 的并发读能力，所有查询都要排队。

**建议**：
- **读操作直接执行，不入队**。DuckDB 的并发读是安全的
- **写操作入队**。串行化写入，消除冲突
- 但这要求读操作"容忍"正在写入的中间状态（DuckDB 的 MVCC 会处理这个）

如果你坚持读也入队，代价是：用户查一只股票的日线要等队列里前面的写任务排完。17:40 批量写入期间，所有查询阻塞。

**问题二：500ms 假设能否成立？**

对于单只股票的单次 UPSERT，500ms 绰绰有余。但当前 `task_post_market_fetch` 是一次性写入 5000 只股票的日线数据，这个操作本身可能就要好几秒。

**解决方案**：拆任务。一次批量拉取完成后，不生成一个巨型写入任务，而是按股票分片：

```python
# 拉取完成后
for chunk in split_by_code(fetched_df, batch_size=100):
    enqueue(DBTask(type="write", table="raw_baostock_day", data=chunk))
```

每个 chunk 的写入在 500ms 内完成。队列串行消费，不会阻塞太久。

但需要考虑：**部分写入可见性问题**。5000 只股票分 50 个 chunk，写到第 30 个 chunk 时用户查询，只看到部分数据。这取决于业务是否容忍——日终批量写入期间用户本来就不应依赖数据的完整性。

**问题三：任务的 result 如何回传？**

写任务：result 只是成功/失败，不需要回传数据。简单。

读任务（如果入队）：result 是一个 DataFrame，可能很大。存在 task 对象里会占内存。如果队列积压，内存会爆。

如果读不入队，这不是问题。

**问题四：队列的持久化**

当前提议的队列是内存中的（Python `queue.Queue` 或类似结构）。如果 daemon 进程崩溃：

- 队列中 pending 的任务丢失
- 已经拉取但还没写入的数据丢失（存在 DataFrame 里，内存中）

这比当前方案更差——当前方案虽然粗糙，但 `insert_daily()` 是拉完立即写的，没有中间缓冲。

**解决方案选项**：
- A：接受丢失。daemon 崩溃后重新跑日终任务，重新拉取
- B：队列持久化（SQLite / DuckDB 单独一张 `_task_queue` 表）
- C：WAL 模式——拉取完成后先写文件（CSV/Parquet），再入队"从文件导入DB"的任务。文件是持久化的，崩溃后可以重放

**建议 C**：这和 Raw 层的文件存储天然吻合——拉取的数据先落盘为文件（Raw 层），再通过队列写入 DuckDB（Clean/Canonical 层）。文件本身就是持久化的任务数据。

**问题五：任务优先级**

当前是 FIFO，但不同任务的紧急程度不同：

| 任务 | 紧急度 | 可容忍延迟 |
|------|--------|-----------|
| 用户查询触发的懒拉取写入 | 高 | <5秒 |
| 日终定时批量写入 | 中 | 分钟级 |
| 策略计算结果写入 | 中 | 分钟级 |
| 历史数据回填 | 低 | 小时级 |

如果日终批量写入分了 50 个 chunk 排在前面，用户的一个懒拉取写入要等 50 个 chunk 排完。

**解决方案**：优先级队列。用户的交互触发的任务优先级高于批量任务。但这引入饥饿问题——如果用户不断触发查询，批量任务永远排不上。

折中：两个队列——交互队列（高优先级）+ 批量队列（低优先级），DB Worker 优先消费交互队列，但每处理 N 个交互任务后处理 1 个批量任务。

**问题六：任务超时和重试**

- 任务执行超时（>500ms）怎么处理？Kill？标记 failed？
- 写入失败（唯一约束冲突、schema 不匹配）重试几次？
- 重试是否需要指数退避？
- 失败任务是否阻塞队列？还是跳过继续下一个？

**问题七：与 FastAPI 请求-响应模型的冲突**

当前 FastAPI 的模式是同步请求-响应：

```python
@app.get("/query/daily")
def query_daily(code, start_date, end_date):
    df = dt.get_daily(code, start_date, end_date)  # 同步返回
    return _df_response(df)
```

如果读也入队，这个接口变成：

```python
@app.get("/query/daily")
def query_daily(code, start_date, end_date):
    task_id = enqueue_read_task(...)
    # 然后呢？轮询等结果？还是返回 task_id 让前端来查？
```

两种模式：
- **同步等待**：接口阻塞等 DB Worker 执行完 → 和直接读没有本质区别，只是多了一层调度开销
- **异步返回**：返回 task_id，前端轮询 → 用户体验变差（原来一次请求拿到数据，现在要两次）

这再次说明**读操作不入队更合理**。

**问题八：事务边界**

当前有些操作需要在同一事务中完成：

- `upsert_stock_info()`：DELETE 全表 + INSERT，必须是原子的
- `write_strategy_result()`：DELETE by PK + INSERT，必须原子
- 未来 Clean → Canonical 的多源合并：可能涉及读多个 Clean 表 + 写 Canonical 表

单个任务可以包含一个事务。但如果一个逻辑操作需要拆成多个任务（比如按 chunk 拆分写入），跨任务之间没有事务保证。写到一半崩溃，数据是不完整的。

**解决方案**：
- 对于需要原子性的操作（如全表替换），不拆分，作为一个任务整体执行
- 对于可容忍部分写入的操作（如日终批量 UPSERT），按 chunk 拆分，接受中间状态
- 在任务结构中增加 `group_id` 字段，同一组的任务可以标记整体状态

#### 修订后的方案建议

综合以上问题，建议对原始提案做以下调整：

```
┌─────────────────────────────────────────────────────┐
│                    DB 操作模型                        │
│                                                      │
│  读操作：直接执行（DuckDB 并发读安全）                  │
│  ├── get_daily()      → 同步读，立即返回              │
│  ├── get_minute()     → 同步读，立即返回              │
│  ├── query_strategy() → 同步读，立即返回              │
│  └── ...                                             │
│                                                      │
│  写操作：入队，DB Worker 串行消费                      │
│  ├── 交互写入（用户触发，高优先级）                     │
│  │   └── 懒拉取完成后的写入                           │
│  ├── 批量写入（定时任务，低优先级，可拆 chunk）         │
│  │   └── 日终批量拉取后的写入                          │
│  └── 策略写入（计算完成后，中优先级）                   │
│                                                      │
│  任务状态：内存 + _task_queue 表（持久化）              │
│  任务结果：写任务只记录成功/失败；读任务不入队           │
│  事务：单个写任务 = 一个事务，需要原子性的操作不拆分     │
└─────────────────────────────────────────────────────┘
```

**关键改动**：
1. **读不入队，写才入队**——保留 DuckDB 并发读能力，不牺牲查询延迟
2. **写队列分优先级**——交互写入 > 策略写入 > 批量写入
3. **大任务拆 chunk**——批量写入按股票分片，每个 chunk < 500ms
4. **原子操作不拆分**——全表替换等操作作为一个整体任务
5. **任务持久化**——`_task_queue` 表记录所有写任务的状态，崩溃可恢复

#### 与三层架构的结合

```
数据拉取（网络I/O，耗时不确定）
  │ 产出 DataFrame
  ↓
Raw 层落盘（文件系统写，不入DB队列）
  │ meta.json + tables/*.csv + 原始文件
  ↓
创建 DB 写任务（入队）
  ├── DBTask(type="write", table="raw_xxx_index", data=meta)    # 元数据索引
  └── DBTask(type="write", table="clean_xxx_day", data=df)      # 清洗后数据
  │
  ↓ DB Worker 串行消费
  │
DuckDB（Raw 索引 + Clean + Canonical）
  │
  ↓ 读操作直接查询（不入队）
  │
业务层（策略/指标/前端）
```

数据拉取 → Raw 层文件落盘 → 创建 DB 写任务 → DB Worker 消费写入。拉取阶段完全不碰 DB，落盘完成后才入队。文件落盘是持久化的（解决问题四），即使 daemon 崩溃，文件还在，可以重建 DB 写任务。

#### 剩余漏洞与建议

**漏洞一（已解决）：懒拉取体验**

最初担心写操作入队后，用户查到空数据时拿不到结果。但实际上这是多虑了——数据已经从 BaoStock 拉回内存，不需要等 DB 写入就能返回给用户：

```
懒拉取流程（新方案）：

1. 用户查询 → 读 DB → 发现缺数据
2. 调 BaoStock 拉取 → 得到 DataFrame
3. 立即返回 DataFrame 给用户  ← 数据已在内存，不需要等 DB 写入
4. 异步：文件落盘 + 创建 DB 写任务 → 入队
5. DB Worker 消费 → 写入 DB
```

对比当前方案（拉完先写 DB 再从 DB 读出来返回），新方案甚至更快——省掉了 DB 写入+回读的延迟。

**唯一边界情况：重复拉取。** 第一次拉取返回了数据，但写任务还在队列排队，第二次查询 DB 还是空的，又触发一次拉取。

解决：加内存级"写入中"缓存：

```python
_pending_writes: dict[str, DataFrame] = {}  # key = "daily_bar:sh.600519:2024-01-01:2024-06-07"

def get_daily(code, start, end):
    key = f"daily_bar:{code}:{start}:{end}"
    # 1. 先查 DB
    df = read_from_db(...)
    if not df.empty:
        return df
    # 2. 再查写入中缓存
    if key in _pending_writes:
        return _pending_writes[key]
    # 3. 真正拉取
    df = fetch_from_baostock(...)
    _pending_writes[key] = df
    enqueue_write_task(...)  # 异步入队
    return df
```

DB Worker 写完后清除 `_pending_writes[key]`。确保重复查询不重复拉取，用户始终能拿到数据。

**漏洞二：策略计算 = 读 + 算 + 写，中间状态没有处理**

策略计算的流程是：读行情 → 计算 → 写结果。在新模型下：

```
1. 读行情（直接执行，同步）→ 得到 DataFrame
2. 策略计算（纯 CPU，无 DB）→ 得到结果 DataFrame
3. 写结果（入写队列，异步）
```

步骤 3 是异步的，但步骤 1 的读是基于当前 DB 状态的计算。如果步骤 3 还没执行完，另一个用户又触发了同一个策略的查询，会怎么样？

- 当前方案：`query_strategy()` 检查 `_strategy_meta.last_date`，发现缓存有效，直接读返回
- 新模型：写入还在队列里，`_strategy_meta` 还没更新，另一个请求会认为缓存失效，重复计算

**解决方案**：和懒拉取同理，策略计算结果放入 `_pending_writes` 缓存。查询时先检查缓存再检查 DB。同时在入队时立即更新 `_strategy_meta`（标记 last_date），防止重复计算。或者加策略级内存锁，同一策略同时只允许一次计算。

**漏洞三：DB Worker 单点故障**

DB Worker 是单线程的，如果某个任务导致 Worker 崩溃（比如 DuckDB 内部错误、内存不足），整个写管道停摆。

**建议**：
- DB Worker 加 try/except，任何任务失败都不退出，标记 failed 后继续下一个
- 健康检查：如果 Worker 超过 N 秒没有消费任务，告警
- 崩溃自动重启（daemon 的 lifespan 管理中重启 Worker 线程）

**漏洞四：大 DataFrame 内存占用**

批量拉取 5000 只股票日线，DataFrame 可能有 50MB+。拆 chunk 后每个 chunk 也要几百 KB。如果队列积压 100 个任务，内存占用可观。

**建议**：任务不直接持有 DataFrame，而是持有文件路径。数据先落盘为 Parquet/CSV 文件，任务指向文件，DB Worker 从文件读入再写 DB。这也和 Raw 层文件落盘一致。

```python
# 不好的方式
task = DBTask(data=huge_dataframe)  # DataFrame 常驻内存

# 更好的方式
task = DBTask(data_path="data/tmp/raw_baostock_day_20240607_001.parquet")
# DB Worker: df = pd.read_parquet(task.data_path) → 写 DB → 删除临时文件
```

**漏洞五：DDL 操作的特殊处理**

CREATE TABLE / DROP TABLE / ALTER TABLE 也是写操作，但它们锁的是 catalog 而不是行。如果 DDL 任务和 DML 任务混在同一个队列：

- DDL 可能阻塞后续所有 DML
- DDL 失败（表已存在/不存在）应该不算错误

**建议**：DDL 操作单独处理，不入队列，直接执行（在 daemon 启动时或首次访问时）。队列只处理 DML（INSERT/UPSERT/DELETE）。

**漏洞六：读写的 MVCC 隔离边界**

DuckDB 使用 MVCC，读操作看到的是事务开始时的快照。这意味着：

- 写任务正在执行一个大事务（比如全表替换 stock_info），读操作看到的是旧数据
- 事务提交后，读操作才看到新数据

这其实是**好事**——解决了 stock_info 空窗问题。但有个边界情况：

- 长时间运行的读事务（比如策略计算需要读几秒）可能阻止旧版本被清理，导致内存膨胀
- 需要确保读事务不要持有太久

**建议**：策略计算中，一次性读出所有需要的数据然后释放连接，不要在整个计算过程中持有读事务。

#### 与当前方案的完整对比

| 维度 | 当前方案 | 新方案（读直执 + 写入队） |
|------|---------|-------------------------|
| **写入冲突** | 多线程同时写，依赖 DuckDB 内部锁 | 单 Worker 串行写，零冲突 |
| **读写混合** | 懒拉取把网络I/O和DB写耦合在读路径中 | 读写完全分离，读路径纯查询 |
| **stock_info 空窗** | DELETE+INSERT 之间可读到空表 | MVCC + 事务，读操作始终看到一致快照 |
| **策略写入原子性** | 先DELETE再INSERT，中间可读到空 | 单任务内事务保证 |
| **可观测性** | 几乎没有（last_fetch.json） | 每个写任务有 ID、状态、耗时、错误信息 |
| **写入入口** | 散布在 duckdb_tools / strategy / bs_zone | 统一通过队列，可审计 |
| **崩溃恢复** | 拉完即写，无中间态，崩溃只丢当前批次 | 文件落盘 + 任务持久化，崩溃后可重放 |
| **并发读** | 可以 | 不变，DuckDB 并发读不受影响 |
| **懒拉取体验** | 一次请求拿到数据 | 不变：拉取后直接返回 DataFrame，DB 写入异步进行 |
| **实现复杂度** | 低（散装写入） | 中（队列 + Worker + 任务状态管理） |
| **批量写入** | 一把梭，长事务 | 拆 chunk，可监控进度，可中断恢复 |

**核心收益排序**：

1. **一致性保证**：MVCC + 单写事务，消除所有中间状态可见问题。这是最大的收益
2. **可观测性**：从"黑盒写入"到"每个操作可追踪"。运维和排错能力质的提升
3. **读写分离**：读路径不再有副作用，架构更干净。懒拉取直接返回内存中的 DataFrame，比当前方案更快
4. **崩溃恢复**：文件落盘 + 任务持久化，比当前的"拉完即写"更安全
5. **写入冲突消除**：单 Worker 串行写，不再需要担心多线程写入

**核心代价**：

1. **实现复杂度**：队列、Worker、任务状态管理、优先级、持久化，这些都是新代码
2. **写入延迟**：数据从"拉完即写"变成"拉完入队 → 排队 → 写入"，对交互写入通常增加 <1s，对批量写入影响更大
3. **内存管理**：`_pending_writes` 缓存需要清理机制，否则长期运行会内存泄漏

### 10. ETL 管道总结：你需要回答的问题

| # | 问题 | 影响 |
|---|------|------|
| 1 | 懒拉取还要不要？接受多大延迟？ | 决定查询链路设计 |
| 2 | Raw 层允许 UPSERT 还是只 INSERT？ | 决定数据修正策略 |
| 3 | 部分源失败时，Canonical 层是否部分更新？ | 决定 ETL 调度策略 |
| 4 | Clean/Canonical 是同步还是异步执行？ | 决定日终 ETL 时长 |
| 5 | 复权因子变更后，策略结果是否重算？ | 决定数据一致性保证 |
| 6 | 数据质量校验失败后：阻断 / 标记 / 丢弃？ | 决定数据可靠性 vs 可用性 |
| 7 | 用户即时查询走哪条路径？ | 决定用户体验 |
| 8 | 回填期间正常 ETL 怎么处理？ | 决定迁移方案 |
| 9 | 所有写入是否必须通过 daemon？ | 决定并发安全策略 |
