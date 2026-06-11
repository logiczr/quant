# 回测引擎设计文档

> 版本：v0.1  
> 更新：2026-05-01  
> 状态：设计中

---

## 一、概述

基于 **backtrader** 框架的本地回测引擎，从 DuckDB 读取历史日线数据，模拟交易执行，输出收益曲线、交易记录、统计指标。作为独立增量模块集成到现有量化平台中。

### 设计原则

- **零侵入**：不改动任何现有模块代码
- **数据桥接**：仅通过 `duckdb_tools.get_daily()` 读数据，backtrader 做交易模拟
- **策略独立**：回测策略与现有 `strategy.py` 选股策略是两套体系，可选打通
- **结果结构化**：`BacktestResult` 数据类统一返回，便于前端展示和持久化

---

## 二、架构

### 模块依赖图

```
┌──────────────────────────────────────────────────────┐
│                streamlit_app.py                       │
│           (新增 "🧪 策略回测" 页面)                     │
│                                                       │
│    用户选策略 + 股票 + 日期范围 + 初始资金 + 参数        │
│                     │                                 │
│                     ▼                                 │
│         ┌────────────────────┐                        │
│         │ backtest_engine.py │  ← 新模块               │
│         └──┬─────────────┬──┘                        │
│            │             │                            │
│            ▼             ▼                            │
│    ┌────────────┐  ┌───────────────┐                 │
│    │ duckdb_    │  │  backtrader   │  ← 新依赖         │
│    │ tools.py   │  │  (pip install)│                  │
│    └─────┬──────┘  └───────────────┘                 │
│          │                                            │
│          ▼                                            │
│       DuckDB (daily_bar / index_daily_bar)            │
└──────────────────────────────────────────────────────┘
```

### 与现有模块的依赖关系

| 依赖模块 | 怎么用 | 改动量 |
|---------|--------|--------|
| `duckdb_tools.py` | 读取数据：`get_daily()` / `get_index_daily()` 喂给 backtrader Data Feed | **零改动** |
| `data_tools.py` | 不直接依赖，通过 duckdb_tools 间接用 | **零改动** |
| `strategy.py` | 回测策略与现有策略体系是两套体系（见第四节），可选打通 | **可选扩展** |
| `index_tools.py` | 可选：回测时在指标层面做信号判断 | **零改动** |
| `db_daemon.py` | 可选：注册回测任务 | **零改动** |
| `streamlit_app.py` | 新增回测页面 | **小改** |

---

## 三、核心设计

### 3.1 新文件结构

```
/home/logiczr/quant/
├── backtest_engine.py              # 回测引擎（数据桥接 + 执行 + 结果解析）
├── backtest_strategies/            # 回测策略目录（类似 strategy/ 放 JSON）
│   ├── __init__.py
│   ├── dual_ma.py                  # 双均线策略
│   ├── macd_cross.py               # MACD 金叉死叉策略
│   └── boll_breakout.py            # 布林带突破策略
├── backtest_engine.py              # 引擎主体
└── ... (现有文件不变)
```

### 3.2 backtest_engine.py

#### 数据桥接：DuckDB → backtrader Data Feed

```python
def duckdb_to_feed(
    code: str,
    start_date: str,
    end_date: str,
    adjustflag: str = "3",
) -> bt.feeds.PandasData:
    """
    从 DuckDB 读日线数据，转为 backtrader PandasData。
    
    流程：
      1. 调用 dt.get_daily() 读取日线（含透明补拉）
      2. 列名对齐：volume → vol（backtrader 约定）
      3. 设置 datetime 索引（backtrader 要求）
      4. 返回 bt.feeds.PandasData
    """
    df = dt.get_daily(code, start_date, end_date, adjustflag, auto_fetch=True)
    
    if df.empty:
        raise ValueError(f"{code} 无数据 [{start_date} ~ {end_date}]")
    
    # backtrader 要求
    df = df.rename(columns={"volume": "vol"})
    df["datetime"] = pd.to_datetime(df["date"])
    df = df.set_index("datetime")
    # 只保留 OHLCV
    df = df[["open", "high", "low", "close", "vol"]]
    
    return bt.feeds.PandasData(dataname=df)
```

#### 结果数据类

```python
@dataclass
class BacktestResult:
    """回测结果（结构化输出）"""
    code: str                     # 股票代码
    strategy_name: str            # 策略名称
    start_date: str               # 回测起始日
    end_date: str                 # 回测截止日
    initial_cash: float           # 初始资金
    final_value: float            # 最终净值
    total_return: float           # 总收益率 %
    annual_return: float          # 年化收益率 %
    max_drawdown: float           # 最大回撤 %
    sharpe_ratio: float           # 夏普比率
    trade_count: int              # 交易次数
    win_rate: float               # 胜率 %
    equity_curve: pd.DataFrame    # 净值曲线（date, value）
    trades: pd.DataFrame          # 交易记录（date, action, price, shares, pnl）
```

#### 一键回测入口

```python
def run_backtest(
    strategy_cls: type[bt.Strategy],     # bt.Strategy 子类
    code: str,                            # 股票代码
    start_date: str,                      # 起始日期
    end_date: str,                        # 截止日期
    cash: float = 1_000_000,              # 初始资金
    commission: float = 0.0003,           # 佣金率（万三）
    adjustflag: str = "3",                # 复权方式
    strategy_params: dict | None = None,  # 策略参数
) -> BacktestResult:
    """
    一键回测入口。
    
    流程：
      1. 创建 Cerebro 引擎
      2. 加载策略 + 参数
      3. 加载数据 Feed
      4. 设置 Broker（资金 + 佣金）
      5. 添加分析器（SharpeRatio / DrawDown / TradeAnalyzer / TimeReturn）
      6. 运行回测
      7. 解析分析器结果 → 构建 BacktestResult
    """
    cerebro = bt.Cerebro()
    
    # 策略
    cerebro.addstrategy(strategy_cls, **(strategy_params or {}))
    
    # 数据
    feed = duckdb_to_feed(code, start_date, end_date, adjustflag)
    cerebro.adddata(feed)
    
    # Broker
    cerebro.broker.setcash(cash)
    cerebro.broker.setcommission(commission=commission)
    
    # 分析器
    cerebro.addanalyzer(bt.analyzers.SharpeRatio, _name="sharpe")
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name="drawdown")
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name="trades")
    cerebro.addanalyzer(bt.analyzers.TimeReturn, _name="timereturn")
    
    # 执行
    results = cerebro.run()
    strat = results[0]
    
    # 解析结果
    return _parse_result(strat, code, start_date, end_date, cash)
```

#### 结果解析

```python
def _parse_result(
    strat, code: str, start_date: str, end_date: str, initial_cash: float
) -> BacktestResult:
    """从 backtrader 分析器结果中提取统计指标。"""
    
    # 夏普比率
    sharpe = strat.analyzers.sharpe.get_analysis()
    sharpe_ratio = sharpe.get("sharperatio", 0.0) or 0.0
    
    # 回撤
    dd = strat.analyzers.drawdown.get_analysis()
    max_drawdown = dd.max.drawdown  # %
    
    # 交易分析
    ta = strat.analyzers.trades.get_analysis()
    total_trades = ta.total.closed
    won_trades = ta.won.total
    win_rate = (won_trades / total_trades * 100) if total_trades > 0 else 0.0
    
    # 净值曲线
    timereturn = strat.analyzers.timereturn.get_analysis()
    equity = pd.Series(timereturn).sort_index()
    equity_curve = (1 + equity).cumprod() * initial_cash
    equity_df = pd.DataFrame({
        "date": equity_curve.index,
        "value": equity_curve.values,
    })
    
    final_value = equity_curve.iloc[-1]
    total_return = (final_value / initial_cash - 1) * 100
    
    # 年化收益
    days = (pd.Timestamp(end_date) - pd.Timestamp(start_date)).days
    annual_return = ((final_value / initial_cash) ** (365 / max(days, 1)) - 1) * 100
    
    # 交易记录（从 strat 闭合交易中提取）
    trades_list = []
    for trade in strat._trades:  # backtrader 内部属性
        trades_list.append({
            "date": trade.dtclose,
            "action": "BUY" if trade.islong else "SELL",
            "price": trade.price,
            "shares": trade.size,
            "pnl": trade.pnl,
        })
    trades_df = pd.DataFrame(trades_list) if trades_list else pd.DataFrame(
        columns=["date", "action", "price", "shares", "pnl"]
    )
    
    return BacktestResult(
        code=code,
        strategy_name=strat.__class__.__name__,
        start_date=start_date,
        end_date=end_date,
        initial_cash=initial_cash,
        final_value=round(final_value, 2),
        total_return=round(total_return, 2),
        annual_return=round(annual_return, 2),
        max_drawdown=round(max_drawdown, 2),
        sharpe_ratio=round(sharpe_ratio, 4),
        trade_count=total_trades,
        win_rate=round(win_rate, 2),
        equity_curve=equity_df,
        trades=trades_df,
    )
```

### 3.3 策略定义

每个策略是一个 `bt.Strategy` 子类，放在 `backtest_strategies/` 目录下。

#### 策略基类（可选）

```python
class SignalStrategy(bt.Strategy):
    """
    信号策略基类。
    
    子类只需实现 generate_signal() 返回 1(买) / -1(卖) / 0(无信号)，
    基类处理仓位管理。
    """
    params = (
        ("stake", 100),          # 每次买卖股数
        ("print_log", False),
    )
    
    def generate_signal(self) -> int:
        """子类实现：返回 1=买入, -1=卖出, 0=无操作"""
        raise NotImplementedError
    
    def next(self):
        signal = self.generate_signal()
        if signal > 0 and not self.position:
            self.buy(size=self.p.stake)
        elif signal < 0 and self.position:
            self.sell(size=self.p.stake)
```

#### 示例策略：双均线

```python
# backtest_strategies/dual_ma.py
import backtrader as bt

class DualMAStrategy(bt.Strategy):
    """双均线交叉策略"""
    
    params = (
        ("fast", 5),
        ("slow", 20),
        ("stake", 100),
    )
    
    def __init__(self):
        self.fast_ma = bt.ind.SMA(self.data.close, period=self.p.fast)
        self.slow_ma = bt.ind.SMA(self.data.close, period=self.p.slow)
        self.crossover = bt.ind.CrossOver(self.fast_ma, self.slow_ma)
    
    def next(self):
        if not self.position:
            if self.crossover > 0:    # 金叉
                self.buy(size=self.p.stake)
        else:
            if self.crossover < 0:    # 死叉
                self.sell(size=self.p.stake)
```

#### 示例策略：MACD 金叉死叉

```python
# backtest_strategies/macd_cross.py
import backtrader as bt

class MACDCrossStrategy(bt.Strategy):
    """MACD 金叉死叉策略"""
    
    params = (
        ("macd_fast", 12),
        ("macd_slow", 26),
        ("macd_signal", 9),
        ("stake", 100),
    )
    
    def __init__(self):
        self.macd = bt.ind.MACD(
            self.data.close,
            period_me1=self.p.macd_fast,
            period_me2=self.p.macd_slow,
            period_signal=self.p.macd_signal,
        )
        self.crossover = bt.ind.CrossOver(self.macd.macd, self.macd.signal)
    
    def next(self):
        if not self.position:
            if self.crossover > 0:
                self.buy(size=self.p.stake)
        else:
            if self.crossover < 0:
                self.sell(size=self.p.stake)
```

#### 示例策略：布林带突破

```python
# backtest_strategies/boll_breakout.py
import backtrader as bt

class BollBreakoutStrategy(bt.Strategy):
    """布林带突破策略：价格突破上轨买入，跌破下轨卖出"""
    
    params = (
        ("period", 20),
        ("devfactor", 2.0),
        ("stake", 100),
    )
    
    def __init__(self):
        self.boll = bt.ind.BollingerBands(
            self.data.close,
            period=self.p.period,
            devfactor=self.p.devfactor,
        )
    
    def next(self):
        if not self.position:
            if self.data.close > self.boll.top:
                self.buy(size=self.p.stake)
        else:
            if self.data.close < self.boll.bot:
                self.sell(size=self.p.stake)
```

### 3.4 策略自动发现

类似 `strategy.py` 的 `list_strategies()`，自动扫描 `backtest_strategies/` 目录：

```python
def list_backtest_strategies() -> list[dict]:
    """
    扫描 backtest_strategies/ 目录，返回所有策略类信息。
    
    返回:
        [
            {"name": "DualMAStrategy", "module": "dual_ma", 
             "params": {"fast": 5, "slow": 20, "stake": 100}},
            ...
        ]
    """
    strategies = []
    pkg_dir = Path(__file__).parent / "backtest_strategies"
    
    for f in sorted(pkg_dir.glob("*.py")):
        if f.name.startswith("_"):
            continue
        module_name = f.stem
        spec = importlib.util.spec_from_file_location(module_name, f)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # 找所有 bt.Strategy 子类
        for attr_name in dir(module):
            attr = getattr(module, attr_name)
            if (isinstance(attr, type) 
                and issubclass(attr, bt.Strategy) 
                and attr is not bt.Strategy):
                strategies.append({
                    "name": attr_name,
                    "module": module_name,
                    "params": {k: v for k, v in attr.params._getitems()},
                })
    
    return strategies


def load_strategy_class(name: str) -> type[bt.Strategy]:
    """按策略名加载 bt.Strategy 子类。"""
    for s in list_backtest_strategies():
        if s["name"] == name:
            spec = importlib.util.spec_from_file_location(
                s["module"], 
                Path(__file__).parent / "backtest_strategies" / f"{s['module']}.py"
            )
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            return getattr(module, name)
    raise ValueError(f"回测策略不存在: {name}")
```

---

## 四、回测策略 vs 现有策略体系

| 维度 | 现有 strategy.py | 回测引擎 |
|------|-----------------|---------|
| 目的 | 选股打分/排行 | 模拟交易，验证买卖信号 |
| 输出 | DataFrame（code + rank + 因子值） | BacktestResult（收益/回撤/交易记录） |
| 数据流 | 读 daily_bar → 计算 → 写 strategy_xxx 表 | 读 daily_bar → 喂 backtrader → 输出结果 |
| 缓存 | last_date 机制 | 无缓存（每次重跑） |
| 策略定义 | JSON + Python 函数 | bt.Strategy 子类 |
| 持久化 | 写入 DuckDB 策略表 | 可选写入 backtest_result 表 |

### 可选打通方式

1. **选股 → 回测**：先跑 `strategy.py` 的选股策略得出股票池，再对池中股票逐一回测
   ```python
   # 示例：市值 Top50 → 双均线回测
   cap_df = se.query_strategy("market_cap_rank", date="2026-04-30")
   top50 = cap_df.head(50)["code"].tolist()
   for code in top50:
       result = run_backtest(DualMAStrategy, code, "2025-01-01", "2026-04-30")
   ```

2. **指标复用**：回测策略中可以直接使用 `index_tools` 预计算的指标
   ```python
   # 在 Strategy.__init__ 中读 indicators 表
   # （需要额外实现 indicators 数据 Feed）
   ```

---

## 五、Streamlit 回测页面

在 `streamlit_app.py` 侧边栏增加 `"🧪 策略回测"` 页面：

```python
elif page == "🧪 策略回测":
    st.title("策略回测")
    
    # ── 策略选择 ──
    strategies = list_backtest_strategies()
    strategy_names = [s["name"] for s in strategies]
    selected_name = st.selectbox("选择回测策略", strategy_names)
    
    # ── 股票代码 ──
    code = st.text_input("股票代码", "sh.600519")
    
    # ── 日期范围 ──
    col1, col2 = st.columns(2)
    with col1:
        start = st.date_input("起始日期", pd.Timestamp.today() - pd.Timedelta(days=365))
    with col2:
        end = st.date_input("截止日期", pd.Timestamp.today())
    
    # ── 资金 & 佣金 ──
    col3, col4 = st.columns(2)
    with col3:
        cash = st.number_input("初始资金 (万元)", value=100, min_value=1)
    with col4:
        commission = st.number_input("佣金率 (万)", value=3, min_value=0) / 10000
    
    # ── 策略参数 ──
    selected_info = next(s for s in strategies if s["name"] == selected_name)
    params = {}
    if selected_info["params"]:
        st.subheader("策略参数")
        for key, default_val in selected_info["params"].items():
            if key == "stake":
                continue  # stake 用全局设置
            params[key] = st.number_input(key, value=default_val)
    
    # ── 执行回测 ──
    if st.button("开始回测", type="primary"):
        with st.spinner("回测中..."):
            strategy_cls = load_strategy_class(selected_name)
            result = run_backtest(
                strategy_cls=strategy_cls,
                code=code,
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d"),
                cash=cash * 10000,
                commission=commission,
                strategy_params=params or None,
            )
        
        # ── 结果展示 ──
        st.subheader("回测结果")
        
        # 指标卡片
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("总收益率", f"{result.total_return:+.2f}%")
        col2.metric("年化收益", f"{result.annual_return:+.2f}%")
        col3.metric("最大回撤", f"{result.max_drawdown:.2f}%")
        col4.metric("夏普比率", f"{result.sharpe_ratio:.2f}")
        
        col5, col6, col7 = st.columns(3)
        col5.metric("交易次数", f"{result.trade_count}")
        col6.metric("胜率", f"{result.win_rate:.1f}%")
        col7.metric("最终净值", f"{result.final_value:,.0f}")
        
        st.divider()
        
        # 净值曲线
        st.subheader("净值曲线")
        st.line_chart(result.equity_curve.set_index("date")["value"])
        
        # 交易记录
        st.subheader("交易记录")
        st.dataframe(result.trades, use_container_width=True, height=400)
```

---

## 六、可选扩展：回测结果持久化

新增 `backtest_result` 表，存储历史回测结果，支持对比不同策略/参数：

```sql
CREATE TABLE IF NOT EXISTS backtest_result (
    id          INTEGER PRIMARY KEY DEFAULT nextval('backtest_result_seq'),
    code        VARCHAR NOT NULL,
    strategy    VARCHAR NOT NULL,
    start_date  DATE    NOT NULL,
    end_date    DATE    NOT NULL,
    cash        DOUBLE,
    params_json VARCHAR,            -- 策略参数 JSON
    total_return   DOUBLE,
    annual_return  DOUBLE,
    max_drawdown   DOUBLE,
    sharpe_ratio   DOUBLE,
    trade_count    INTEGER,
    win_rate       DOUBLE,
    equity_json    VARCHAR,         -- 净值曲线 JSON
    trades_json    VARCHAR,         -- 交易记录 JSON
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

使用场景：
- 回测完成后可选保存到 `backtest_result`
- Streamlit 页面增加"历史回测"标签页，展示对比表

---

## 七、新增依赖

```
# requirements.txt 新增
backtrader          # 回测框架
matplotlib          # backtrader 内部绑定的绘图库（可选，用于生成回测图表）
```

安装：
```bash
pip install backtrader matplotlib
```

---

## 八、开发计划

| 阶段 | 内容 | 优先级 |
|------|------|--------|
| P0 | `backtest_engine.py` 核心代码（数据桥接 + run_backtest + BacktestResult） | 必须 |
| P0 | 3 个示例策略（双均线 / MACD / 布林带） | 必须 |
| P0 | Streamlit 回测页面 | 必须 |
| P1 | 策略自动发现 + 参数表单 | 重要 |
| P1 | 回测结果持久化（backtest_result 表） | 重要 |
| P2 | 与 strategy.py 打通（选股池 → 批量回测） | 可选 |
| P2 | 回测结果对比（多策略/多参数横向对比） | 可选 |
| P2 | backtrader 内置绘图（matplotlib K 线 + 买卖点标注） | 可选 |
| P3 | 多股组合回测（Portfolio 级别） | 远期 |
| P3 | 自定义 Broker（涨停买不进/跌停卖不出等 A 股规则） | 远期 |
