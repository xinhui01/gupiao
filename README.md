# A股筛选

这是一个基于 Python 的日终 A 股筛选工具，主要用于：

- 扫描全量 A 股股票
- 按你设置的条件筛选结果
- 查看单只股票的历史走势
- 导出扫描结果

当前版本已经改成了“收盘后批处理”思路，不再依赖实时行情作为主流程。
同时加入了更保守的东方财富访问保护，默认优先复用本地缓存，避免高并发连续扫描后触发限流。
历史日线现已支持多源切换，可在 `auto / eastmoney / tencent / sina` 之间切换。
推荐流程已调整为“先更新历史缓存，再执行本地扫描”。

最近一次迭代的重点是稳定性与可取消性：引入了统一的取消令牌机制，扫描 / 缓存更新 / 详情 / 分时 / 涨停对比 / 涨停预测共 6 类后台任务都接入广播取消；
数据库恢复流程加了安全收口，恢复前会先停掉后台任务并失效所有 SQLite 连接；
Tk 主线程调度收敛到统一的安全入口，关闭窗口不再容易冒出 `TclError`。

> 📖 面向使用者的**功能说明 + 流程图**：[`docs/usage.md`](docs/usage.md)

## 主要功能

- 全量股票池扫描
- 最近 N 日收盘价高于 MA 的筛选
- 近 N 日涨停过滤
- 放量观察
- 结果按多条件排序
- 板块过滤
- 单股详情查看
- 结果导出为 CSV
- 本地 SQLite 持久化保存

## 数据来源

项目默认使用 AKShare 和东方财富公开接口获取数据。

说明：

- 涨停池用于获取涨停列表
- 强势股池用于获取“入选理由”
- 目前不使用任何需要绕过反爬的方式
- 如果某个来源拿不到数据，会自动降级，不影响主流程

## 当前筛选逻辑

一只股票要通过扫描，通常需要满足这些条件：

1. 最近 N 个交易日收盘价高于 MA
2. 可选：最近 N 日内出现过涨停
3. 可选：满足放量条件

其中放量判断方式是：

- 看最近“放量观察天数”内的成交量
- 计算最大值和最小值
- 用 `最大值 / 最小值` 得到放量倍数
- 若勾选启用放量倍数，并且倍数达到阈值，就记为放量

## 界面说明

主界面提供了这些参数：

- 扫描数量
- 并发线程数
- 连续天数
- MA 周期
- 近 N 日涨停
- 放量观察天数
- 启用放量倍数
- 放量倍数阈值
- 板块过滤

结果表支持点击表头排序。

说明：

- 默认并发线程已经下调到 `3`
- 实际扫描并发会自动服从历史接口保护上限，不会无限按你输入的线程数去打外部接口
- 当东方财富返回疑似限流/封禁信号时，程序会主动进入冷却期，并优先回退本地缓存
- 每轮扫描前后会输出一组运行诊断日志，帮助判断是缓存命中不足、网络失败，还是触发了限流保护
- 历史数据源支持 `auto / eastmoney / tencent / sina`；`auto` 会优先东财，失败后切换到腾讯和新浪
- 新增“更新历史缓存”任务，适合首次拉新和每天收盘后的增量补齐
- 全量扫描默认只读本地历史缓存，不再逐只联网抓历史
- “停止”按钮现在会真正传播到后台任务（不再只是翻布尔标记）：正在跑的历史拉取、详情加载、涨停对比等都会在就近检查点收到取消信号并尽快退出
- 数据库备份 / 恢复入口做了安全收口：恢复前会先要求用户停掉扫描，再广播取消、关闭所有 SQLite 连接后才覆盖文件；覆盖失败时会尝试回滚到恢复前备份

## 本地存储

程序会把数据保存到：

- `data/stock_store.sqlite3`

里面保存了：

- 股票池
- 历史日线
- 扫描结果快照

启动时会自动检查并创建数据库文件和表结构。

## 运行方式

```bash
python main.py
```

如果是第一次运行，建议先确认依赖已安装：

```bash
pip install -r requirements.txt
```

## 在新机器上快速运行

推荐使用虚拟环境，这样最省心，也不容易和系统里其它 Python 项目互相影响。

Windows：

```bash
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python main.py
```

macOS / Linux：

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python main.py
```

建议：

- 尽量使用较新的 Python 3 版本
- 如果更换了系统或环境，优先重新创建 `.venv`
- 首次启动会自动创建本地数据库文件 `data/stock_store.sqlite3`

## 跨系统运行说明

本项目是桌面 GUI 程序，依赖 `tkinter`。

- Windows 上通常可直接运行
- Linux 上如果缺少 `tkinter`，通常需要额外安装 `python3-tk`
- macOS 上一般使用官方 Python 安装包即可正常带上 `tkinter`

如果遇到启动时报错，先确认：

- Python 版本正常
- `pip install -r requirements.txt` 已执行成功
- 当前命令使用的是项目自己的虚拟环境解释器

## 打包为可执行文件

如果希望发给没有 Python 环境的电脑使用，可以在对应系统上使用 `PyInstaller` 打包。

安装：

```bash
pip install pyinstaller
```

Windows 打包示例：

```bash
pyinstaller -w -F main.py -n ashare-scan
```

打包完成后，可执行文件会出现在 `dist/` 目录中。

注意：

- Windows 程序建议在 Windows 上打包
- macOS 程序建议在 macOS 上打包
- Linux 程序建议在 Linux 上打包
- 一般不能做到“在一个系统打包后直接在所有系统通用”

## 项目结构

顶层模块（薄 facade 居多，主要逻辑已模块化到 `src/`）：

| 文件 | 说明 |
|---|---|
| `main.py` | 程序入口 |
| `stock_gui.py` | Tkinter 图形界面（排序 / 导出 / 详情 / 取消） |
| `stock_filter.py` | 筛选逻辑、评分、涨停预测、放量判断 |
| `stock_data.py` | 数据抓取统一入口（facade，转发给 `src/sources/*`）+ `EODData` 主类 |
| `stock_store.py` | SQLite 存储层（schema / 读写 / 连接管理 / 备份恢复） |
| `stock_validator.py` | OHLC / 涨跌幅 / 交易日缺口校验 |
| `stock_logger.py` | 统一 logging 配置 |
| `stock_indicators.py` | 技术指标 |
| `data_source_models.py` | 多源切换的数据类（`DataProviderPlan` 等） |
| `llm_client.py` | NVIDIA NIM 在线推理 client |
| `llm_theme_clustering.py` | 题材聚类 |
| `quote_channel.py` | 行情分片占位 |
| `scan_models.py` | 扫描相关数据类 |
| `requirements.txt` | Python 依赖 |

`src/` 按职责模块化（这一轮重构把 `stock_data.py` 从 4417 行压到约 2076 行，多出来的逻辑都搬到这里）：

```
src/
├── config.py                         统一 env 读取（env_int / env_float / env_bool / env_str）
├── network/                          HTTP 网络层（跨源共用）
│   ├── headers.py                        UA / Referer 池 + 东财随机 headers/cookie
│   ├── proxy_pool.py                     可选免费代理池（多源拉取 + 验证 + 黑名单）
│   └── host_health.py                    全局主机健康/冷却管理（被所有源共享）
├── sources/                          数据源
│   ├── _common.py                        市场前缀 / 历史 frame 标准化 / 列名匹配
│   ├── _jsonp.py                         JSONP 回调名 + 包装剥离（跨源共用）
│   ├── universe.py                       A 股全市场列表构建（深 + 沪含科创板）
│   ├── tencent.py                        腾讯证券历史日线（自建直连 + akshare 回退）
│   ├── sina.py                           新浪财经
│   ├── netease.py                        网易财经（CSV / GBK）
│   ├── baidu.py                          百度股市通
│   ├── sohu.py                           搜狐财经（JSONP）
│   ├── ths.py                            同花顺 10jqka（按年请求合并）
│   ├── wscn.py                           华尔街见闻
│   └── eastmoney/                    东方财富抓取链路（独立子包，依赖关系无环）
│       ├── throttling.py                     自适应间隔 + 阻塞冷却 + 异常类 + 诊断计数
│       ├── rate_limit.py                     限流信号检测
│       ├── session.py                        核心 GET 包装（限流/代理/JSONP/headers 组合点）
│       ├── history_parser.py                 request_params + parse_hist_json
│       ├── history.py                        probe_mirror + fetch_hist_frame（顶层入口）
│       ├── intraday.py                       盘前竞价快照 + 1min 分时 + 帧标准化/选日/切日
│       ├── mirrors.py                        push2 多节点 URL 构造 + 健康度优先级
│       ├── numeric.py                        行情字段还原（"元×1000" 修正）
│       ├── akshare_patch.py                  替换 akshare 内置 request_with_retry
│       └── akshare_warnings.py               静默 akshare concept-board 噪声警告
├── utils/                            通用工具
│   ├── codes.py                          股票代码 / 板块 / 交易所推断
│   ├── parsing.py                        safe_float / 中文数字 / 概念字符串归一化 / 列名匹配
│   ├── cache_freshness.py                今日/最近交易日估算 + 历史缓存新鲜度判定
│   ├── lru_cache.py                      OrderedDict 实现的 LRU
│   ├── cancel_token.py                   `CancelToken` + `CancelTokenRegistry`：统一的可传播取消机制
│   ├── daemon_executor.py                `DaemonThreadPoolExecutor`：worker 强制为 daemon 的线程池
│   ├── em_circuit_breaker.py             东方财富请求熔断器（连续失败指数退避）
│   ├── trade_calendar.py                 交易日历推导
│   └── snapshot_history.py               扫描快照相关的历史辅助
├── services/                         业务服务
│   ├── store_facade.py                   Store 包装层（universe / history / fund_flow 各 save+load）
│   ├── db_admin_service.py               数据库备份 / 恢复 / 清理 / CSV 导入导出 + `SafeRestoreOrchestrator`
│   └── history_analysis_service.py       历史 K 线分析（MA / 趋势 / 涨停识别 / 评分）
├── gui/
│   ├── ui_dispatch.py                    `UIDispatcher`：后台线程 → Tk 主线程的安全派发
│   ├── log_drainer.py                    `LogDrainer`：日志队列 + 定时抽取 + 主线程直写
│   ├── result_columns.py                 结果列定义
│   └── result_filters.py                 结果过滤器
└── models/
    └── analysis_models.py                分析相关的数据类
```

老的调用方（如 `from stock_data import _retry_ak_call`、`from stock_store import backup_database` 等）都保留了别名转发，升级后无需改动任何业务代码。

## 测试

```bash
.venv/Scripts/python -m pytest -q          # Windows
# 或
.venv/bin/python -m pytest -q              # macOS / Linux
```

当前测试覆盖主要围绕本次稳定性改造展开：

- 取消机制：`CancelToken` / `CancelTokenRegistry` / `coerce_should_stop`
- 数据库恢复：写锁串行化、连接失效、恢复失败回滚、备份文件缺失
- 线程 → UI 派发：`UIDispatcher` 在关闭 / `winfo_exists` 抛错 / `TclError` 下的防御
- 日志 drainer：主线程直写、其它线程入队、关闭时短路、sink 异常不冒泡
- 东财熔断器：阈值触发、指数退避、max cooldown 封顶、并发安全
- 守护线程池：worker 是 daemon、异常透传到 future
- 扫描/缓存取消：token 预取消跳过任务、等待中取消快速回 fallback

## 升级到本版本（已有数据库）

本版本在 `history_meta` 表上新增了 3 个列。**全新安装无需任何操作**，
`CREATE TABLE IF NOT EXISTS` 会直接建出含这 3 列的完整表。

如果是从旧版本升级，应用首次启动时会自动执行 `ALTER TABLE` 幂等补列。
如果你需要在服务器或只读复制库上手工同步执行（本地已自动处理），DDL 如下：

```sql
ALTER TABLE history_meta ADD COLUMN partial_fields TEXT NOT NULL DEFAULT '';
ALTER TABLE history_meta ADD COLUMN needs_repair INTEGER NOT NULL DEFAULT 0;
ALTER TABLE history_meta ADD COLUMN source_failure_streak INTEGER NOT NULL DEFAULT 0;
```

注意：这 3 条 `ALTER` 不幂等，**只在首次升级时执行一次**；应用代码内部用
`PRAGMA table_info` 做了存在性检查，重复启动程序本身是安全的。

## 备注

- 这是一个日终筛选工具，适合每天收盘后使用
- 当前的“涨停原因”主要来自东方财富强势股池的“入选理由”
- 如果拿不到原因，会留空，而不是编造规则说明
- 扫描结果是可以恢复的，下次启动后可直接读取本地快照

## 免责声明

本项目仅供学习和研究使用，不构成任何投资建议。市场有风险，使用前请自行判断。
