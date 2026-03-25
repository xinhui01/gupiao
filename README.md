# 股票监控程序

基于 Python 的小型桌面工具，从 **A 股**实时行情中按自定义条件筛选股票，查看详情与近期走势图，并可将扫描结果导出为 CSV。

数据来源：[AKShare](https://github.com/akfamily/akshare)（东方财富等公开接口）。**仅供学习与研究，不构成任何投资建议。**

## 功能概览

- **批量扫描**：从全市场列表中按「扫描数量」依次检测，仅保留通过条件的标的。
- **单股查询**：输入股票代码，在「股票详情」页查看实时字段、资金流向、内外盘与近期收盘价走势图（Matplotlib 嵌入界面）。
- **导出**：菜单「文件 → 导出结果」将当前扫描结果保存为 UTF-8 BOM 的 CSV，便于 Excel 打开。

## 扫描性能（大量股票时）

阶段 3 对每只标的有多路网络请求（行情、资金流、K 线等），**完全串行**时 5000 只会极慢。当前实现使用 **线程池并发**：

- 界面 **「并发线程」**（默认 12，范围约 1～64），或环境变量 **`GUPPIAO_SCAN_WORKERS`**（未开 GUI 时生效）。  
- 可适当调到 **16～24** 缩短总时间；再往上容易触发数据源限流、断连或临时封禁，需自行把握。  
- 仍慢时：减少「扫描数量」、降低筛选条件通过率、或换付费/注册 API（见上文「可注册数据源」）。

## 筛选逻辑说明

当前实现中，一只股票**通过**扫描需同时满足（具体见 `stock_filter.py`）：

1. **价格区间**：现价在界面设置的最低价～最高价之间。  
2. **量比**：不低于设定的量比阈值。  
3. **内外盘比**：外盘/内盘之比不低于设定阈值。  
4. **均线**：最近 **N** 个交易日（默认 5，与 MA 周期一致时可表示「连续 5 天收盘均在 5 日均线之上」）每一天的**收盘价均高于**当日 **MA5**（5 日收盘均价）。MA 按常规用含当日的过去 5 个交易日收盘计算。

说明：

- **默认股票池**：不再拉取东方财富 `clist` 分页全表（页数多、极易限流卡住）。改为 **深交所 + 上交所（含科创板）** 官方列表合并，**不含北交所**；请求量少、通常几十秒内可完成建池。  
- **股票池本地缓存**：首次从交易所拉取后会写入项目下 `cache/a_share_universe.csv`（默认 **168 小时**内再次扫描直接读缓存，不再请求列表）。环境变量 **`GUPPIAO_UNIVERSE_CACHE_HOURS`** 可改有效期；**`GUPPIAO_REFRESH_UNIVERSE=1`** 或界面勾选「重新拉取股票池」可强制重建；菜单「设置 → 清除股票池本地缓存」可删除文件。  
- **逐只行情**：默认用东方财富单股接口 `stock_bid_ask_em`（`api/qt/stock/get`），与全表分页无关；同一只在单次筛选流程内会缓存，减少重复请求。  
- **旧版全表模式**（不推荐）：若仍要强行走东方财富分页全表，可设环境变量 `GUPPIAO_LIST_SOURCE=em`，恢复按量比预筛；易长时间卡住。  
- 界面中的「主力净流入(万)」会参与参数保存与结果说明文字；当前代码里**未将其作为硬性否决条件**，详情中仍会展示资金流向。

### 若需更稳定/可商用的数据源（可自行注册）

以下为常见可注册接口，**与本项目无绑定**，需自行接入代码：  

- **[Tushare Pro](https://tushare.pro/)**：注册得 token，`stock_basic` 取列表，行情/资金流等有积分与权限限制，文档齐全。  
- **[Baostock](http://baostock.com/)**：偏历史与基础数据，注册简单，适合回测类需求。  
- **聚宽 / 米筐 / Wind 等**：机构或量化常用，按各自协议与费用。  

当前仓库仍以 AKShare 免费接口为主；要完全摆脱东财限速，长期更合适做法是：**列表 + 行情统一走你已购买/已注册的 API**，在 `stock_data.py` 中替换 `get_all_stocks` / 行情解析即可。

### 多数据源分组（新浪 / 同花顺 / 雪球等）

把全市场代码分成几组、分别请求不同网站，**理论上**可以分摊限流；但本项目的筛选条件依赖 **东方财富特有的字段组合**（如统一口径的量比、内外盘、主力流）。新浪串行情、同花顺/雪球在 AKShare 中的多数接口**无法无改代码直接替换**同一套 dict。  

若要做真正的多源加速，需要：按源实现与当前 `filter_stock` 兼容的数据结构，缺失字段则对该组回退东财或调整规则。仓库中 `quote_channel.py` 仅预留按代码哈希分片的入口（`GUPPIAO_QUOTE_SHARDS`，默认 1），便于后续自行接线路由。

## 环境要求

- **Python 3.9+**（建议 3.10 或以上）  
- **操作系统**：已在 Windows 上配合 `tkinter` + `TkAgg` 使用；Linux/macOS 需本机具备 Tk 与相应中文字体时，图表中文显示更正常。  
- **网络**：运行时需能访问 AKShare 所依赖的数据源。

## 安装

```bash
cd /path/to/gupiao
python -m venv .venv
```

**Windows（PowerShell）** 激活虚拟环境：

```powershell
.\.venv\Scripts\Activate.ps1
```

**macOS / Linux**：

```bash
source .venv/bin/activate
```

安装依赖：

```bash
pip install -r requirements.txt
```

依赖包括：`akshare`、`pandas`、`numpy`、`matplotlib`。

## 运行

```bash
python main.py
```

启动后为「股票监控程序」主窗口：在控制面板设置阈值后点击「开始扫描」，可随时「停止」；在结果表中单击或双击股票可刷新详情与图表。

## 项目结构

| 文件 | 说明 |
|------|------|
| `main.py` | 程序入口，创建 Tk 根窗口 |
| `stock_gui.py` | 界面、线程扫描、图表与导出 |
| `stock_filter.py` | 筛选条件与扫描编排 |
| `stock_data.py` | 通过 AKShare 拉取列表、实时、历史、资金流、内外盘；股票池磁盘缓存 |
| `quote_channel.py` | 多源分片扩展预留（默认未启用） |
| `requirements.txt` | Python 依赖版本下限 |

## 常见问题

- **SSL 证书校验失败**（如 `CERTIFICATE_VERIFY_FAILED`、`self-signed certificate in certificate chain`）：多见于公司 HTTPS 代理、杀毒解密 HTTPS、或未信任的企业根证书。可先尝试 `pip install --upgrade certifi`。若仍失败，可在**本机可接受风险**的前提下跳过 HTTPS 证书校验（会降低安全性），任选其一即可：
  - **环境变量**：PowerShell `$env:GUPPIAO_INSECURE_SSL="1"; python main.py`，cmd `set GUPPIAO_INSECURE_SSL=1 && python main.py`
  - **空文件开关**：在项目根目录（与 `main.py` 同级）新建空文件 `USE_INSECURE_SSL` 或 `.gupiao_insecure_ssl`，再运行 `python main.py`，无需每次设环境变量。
  实现上对 `requests` 默认 `verify=False`（AKShare 使用该库），仅改系统 SSL 上下文往往无法生效。
- **代理错误**（如 `ProxyError`、`Unable to connect to proxy`）：说明本机或环境里配置了 `HTTP(S)_PROXY`，但代理无法访问东方财富接口。可让程序**忽略环境代理**（直连外网，适合你在家或未挂公司代理时使用）：
  - 环境变量：`GUPPIAO_BYPASS_PROXY=1` 后启动程序
  - 或在项目根目录新建空文件 `USE_BYPASS_PROXY` 或 `.gupiao_bypass_proxy`
  实现上为 `requests.Session(trust_env=False)`。若必须经公司代理上网，需在代理侧放行 `*.eastmoney.com`，而不是关本开关。
- **连接被对端直接断开**（如 `RemoteDisconnected`、`Connection aborted`）：股票列表分页走 `akshare.utils.func.fetch_paginated_data`，其 `from ... import request_with_retry` 在导入时就绑定了原函数，仅改 `requests.Session` 无效。本项目会**替换** `akshare.utils.func` 与 `akshare.utils.request` 中的 `request_with_retry`：带完整浏览器头、`Connection: close`、**多 push 镜像**（`push2` / `82` / `33`… 轮换）、更长超时与指数退避重试。仍失败时请降低扫描并发、换网络或稍后再试。
- **请求失败或超时**：多为网络或数据源限流，可稍后重试；扫描数量越大，耗时与失败概率越高。  
- **`ModuleNotFoundError: No module named 'pandas'`**（或其它依赖缺失）：说明**当前用来运行 `main.py` 的 Python** 里没装 `requirements.txt`。请先执行 `python -c "import sys; print(sys.executable)"` 确认路径，再对**同一解释器**执行：`python -m pip install -r requirements.txt`。若本机 Conda 已装好包，请用该环境的 Python 启动（例如 `D:\env\conda\python.exe main.py`），或在 IDE 里把解释器选成该环境。
- **图表中文乱码**：`stock_gui.py` 已配置 `SimHei` / `Microsoft YaHei` 等字体，若系统无相应字体，可为 Matplotlib 配置本机已有中文字体。

## 许可证与免责

本仓库为个人学习用示例代码。市场有风险，使用本工具产生的任何决策与后果由使用者自行承担。
