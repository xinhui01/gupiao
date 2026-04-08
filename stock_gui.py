from __future__ import annotations

import csv
import json
import queue
import threading
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import matplotlib
matplotlib.use("TkAgg")

import matplotlib.pyplot as plt
import pandas as pd
import tkinter as tk
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from matplotlib.ticker import FuncFormatter
from tkinter import ttk, messagebox, scrolledtext, filedialog

from scan_models import FilterSettings, ScanRequest
from data_source_models import DATA_SOURCE_OPTIONS
from stock_filter import StockFilter
from stock_data import clear_history_data, clear_universe_data
from stock_store import ensure_store_ready, load_latest_scan_snapshot, load_scan_snapshot, save_scan_snapshot

plt.rcParams["font.sans-serif"] = ["SimHei", "Microsoft YaHei", "Arial Unicode MS"]
plt.rcParams["axes.unicode_minus"] = False


class StockMonitorApp:
    def __init__(self, root: tk.Tk):
        ensure_store_ready()
        self.root = root
        self.root.title("日终股票筛选器")
        self.root.minsize(1280, 820)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self._set_initial_window_geometry()

        self.stock_filter = StockFilter()
        self.stock_filter.set_log_callback(self._log_async)
        self.all_scan_results: List[Dict[str, Any]] = []
        self.filtered_stocks: List[Dict[str, Any]] = []
        self._detail_request_code = ""
        self._detail_loading_code = ""
        self._detail_after_id = None
        self._current_detail_code = ""
        self._intraday_request_code = ""
        self._intraday_loading_code = ""
        self._intraday_request_offset = 0
        self._intraday_request_target_date = ""
        self._intraday_loading_offset = 0
        self._intraday_loading_target_date = ""
        self._intraday_day_offset = 0
        self._intraday_available_dates: List[str] = []
        self._intraday_selected_date = ""
        self._detail_chart_dates: List[str] = []
        self._detail_chart_window_size = 60
        self._detail_chart_window_start = 0
        self._detail_chart_history = None
        self._detail_chart_analysis: Dict[str, Any] = {}
        self._detail_chart_scroll_bound = False
        self._detail_chart_slider_updating = False
        self._detail_chart_dragging = False
        self._detail_chart_drag_moved = False
        self._detail_chart_drag_start_x = 0.0
        self._detail_chart_drag_start_window = 0
        self._detail_chart_click_target_date = ""
        self._detail_chart_loading_more = False
        self._detail_chart_loaded_days = 0
        self._detail_summary_expanded = False
        self._detail_chart_expanded = True
        self._detail_flow_expanded = False
        self.sort_column = "five_day_return"
        self.sort_reverse = True
        self.is_scanning = False
        self.is_updating_cache = False
        self._scan_thread: Optional[threading.Thread] = None
        self._cache_thread: Optional[threading.Thread] = None
        self._active_scan_request: Optional[ScanRequest] = None
        self._run_log_file: Optional[Path] = None
        self._current_scan_allowed_boards: List[str] = []
        self._current_scan_max_stocks: int = 0
        self.result_columns: tuple[str, ...] = ()
        self.result_headings: Dict[str, tuple[str, int]] = {}
        self.result_column_vars: Dict[str, tk.BooleanVar] = {}
        self.result_column_order: List[str] = []
        self.default_result_display_columns: tuple[str, ...] = ()
        self.result_layout_path = Path("data") / "result_columns.json"
        self.board_filter_layout_path = Path("data") / "board_filters.json"
        self.app_settings_path = Path("data") / "app_settings.json"
        self._log_queue: "queue.SimpleQueue[str]" = queue.SimpleQueue()
        self._main_thread_id = threading.get_ident()
        self._is_closing = False

        self.setup_ui()
        self._load_app_settings()
        self._apply_source_preferences()
        self._load_result_column_layout()
        self._load_board_filter_layout()
        self.apply_result_display_columns(save=False)
        self._load_last_results()
        self.root.after(100, self._drain_log_queue)

    def _set_initial_window_geometry(self) -> None:
        default_width = 1440
        default_height = 900

        try:
            screen_width = max(self.root.winfo_screenwidth(), self.root.minsize()[0])
            screen_height = max(self.root.winfo_screenheight(), self.root.minsize()[1])
        except tk.TclError:
            self.root.geometry(f"{default_width}x{default_height}")
            return

        width = min(default_width, screen_width - 120)
        height = min(default_height, screen_height - 120)
        width = max(width, 1280)
        height = max(height, 820)
        x = max((screen_width - width) // 2, 0)
        y = max((screen_height - height) // 2, 0)
        self.root.geometry(f"{width}x{height}+{x}+{y}")

    def setup_ui(self):
        self.setup_menu()

        main_frame = ttk.Frame(self.root, padding="5")
        main_frame.pack(fill=tk.BOTH, expand=True)

        self.setup_control_panel(main_frame)
        self.setup_notebook(main_frame)
        self.setup_status_bar()

    def setup_menu(self):
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)

        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="文件", menu=file_menu)
        file_menu.add_command(label="导出结果 CSV", command=self.export_results)
        file_menu.add_command(label="导出结果图片", command=self.export_results_image)
        file_menu.add_command(label="复制代码", command=self.copy_selected_stock_code_name, accelerator="Ctrl+C")
        file_menu.add_separator()
        file_menu.add_command(label="退出", command=self.on_close)

        setting_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="设置", menu=setting_menu)
        setting_menu.add_command(label="扫描参数", command=self.show_settings)
        setting_menu.add_command(label="清空股票池", command=self.on_clear_universe_data)
        setting_menu.add_command(label="清空历史数据", command=self.on_clear_history_data)

        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="帮助", menu=help_menu)
        help_menu.add_command(label="关于", command=self.show_about)

    def _build_control_scan_params_row(self, control_frame) -> None:
        row1 = ttk.Frame(control_frame)
        row1.pack(fill=tk.X, pady=5)
        ttk.Label(row1, text="扫描数量(0=全量):").pack(side=tk.LEFT, padx=5)
        self.scan_count_var = tk.StringVar(value="0")
        ttk.Entry(row1, textvariable=self.scan_count_var, width=8).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="并发线程:").pack(side=tk.LEFT, padx=5)
        self.scan_workers_var = tk.StringVar(value="3")
        ttk.Entry(row1, textvariable=self.scan_workers_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="连续天数:").pack(side=tk.LEFT, padx=5)
        self.trend_days_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.trend_days_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="MA周期:").pack(side=tk.LEFT, padx=5)
        self.ma_period_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.ma_period_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="近N日涨停:").pack(side=tk.LEFT, padx=5)
        self.limit_up_lookback_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.limit_up_lookback_var, width=6).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="放量观察天数:").pack(side=tk.LEFT, padx=5)
        self.volume_lookback_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.volume_lookback_var, width=6).pack(side=tk.LEFT, padx=5)

        self.volume_expand_enabled_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            row1,
            text="启用放量倍数",
            variable=self.volume_expand_enabled_var,
        ).pack(side=tk.LEFT, padx=8)

        ttk.Label(row1, text="放量倍数阈值:").pack(side=tk.LEFT, padx=5)
        self.volume_expand_factor_var = tk.StringVar(value="2.0")
        ttk.Entry(row1, textvariable=self.volume_expand_factor_var, width=6).pack(side=tk.LEFT, padx=5)

        row1_note = ttk.Frame(control_frame)
        row1_note.pack(fill=tk.X, pady=2)
        ttk.Label(
            row1_note,
            text="备注：放量倍数=最近N天成交量最大值/最小值，勾选“启用放量倍数”后才参与筛选。",
        ).pack(side=tk.LEFT, padx=5)

        self.refresh_universe_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row1,
            text="重新拉取股票池",
            variable=self.refresh_universe_var,
        ).pack(side=tk.LEFT, padx=15)

    def _build_control_scan_params_note_row(self, control_frame) -> None:
        row1_note = ttk.Frame(control_frame)
        row1_note.pack(fill=tk.X, pady=2)
        ttk.Label(
            row1_note,
            text="备注：放量倍数=最近N天成交量最大值/最小值，勾选“启用放量倍数”后才参与筛选。",
        ).pack(side=tk.LEFT, padx=5)

    def _build_control_actions_row(self, control_frame) -> None:
        row2 = ttk.Frame(control_frame)
        row2.pack(fill=tk.X, pady=5)
        self.scan_btn = ttk.Button(row2, text="开始扫描", command=self.start_scan)
        self.scan_btn.pack(side=tk.LEFT, padx=5)

        self.update_cache_btn = ttk.Button(row2, text="更新历史缓存", command=self.start_history_cache_update)
        self.update_cache_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(row2, text="停止", command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        ttk.Label(row2, text="股票代码:").pack(side=tk.LEFT, padx=5)
        self.stock_code_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self.stock_code_var, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(row2, text="查询股票", command=self.query_single_stock).pack(side=tk.LEFT, padx=15)
        ttk.Label(row2, text="历史源:").pack(side=tk.LEFT, padx=(8, 4))
        self.history_source_var = tk.StringVar(value="auto")
        self.history_source_combo = ttk.Combobox(
            row2,
            textvariable=self.history_source_var,
            width=12,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["history"],
        )
        self.history_source_combo.pack(side=tk.LEFT, padx=4)
        self.history_source_combo.bind("<<ComboboxSelected>>", self.on_history_source_changed)
        ttk.Button(row2, text="列表列设置", command=self.show_column_picker).pack(side=tk.LEFT, padx=8)

        self.intraday_source_var = tk.StringVar(value="auto")
        self.fund_flow_source_var = tk.StringVar(value="auto")
        self.limit_up_reason_source_var = tk.StringVar(value="auto")

    def _build_control_board_filter_row(self, control_frame) -> None:
        row3 = ttk.Frame(control_frame)
        row3.pack(fill=tk.X, pady=5)
        ttk.Label(row3, text="显示板块:").pack(side=tk.LEFT, padx=5)
        self.board_filter_vars = {
            "上交所主板": tk.BooleanVar(value=True),
            "深交所主板": tk.BooleanVar(value=True),
            "创业板": tk.BooleanVar(value=True),
            "科创板": tk.BooleanVar(value=True),
        }
        for label in ("上交所主板", "深交所主板", "创业板", "科创板"):
            ttk.Checkbutton(
                row3,
                text=label,
                variable=self.board_filter_vars[label],
                command=self.on_board_filter_changed,
            ).pack(side=tk.LEFT, padx=8)

        self.require_limit_up_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row3,
            text="仅显示近N日内有涨停",
            variable=self.require_limit_up_var,
        ).pack(side=tk.LEFT, padx=18)

        self.ignore_result_snapshot_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row3,
            text="忽略本地结果快照",
            variable=self.ignore_result_snapshot_var,
        ).pack(side=tk.LEFT, padx=18)

    def _build_control_price_filter_row(self, control_frame) -> None:
        row4 = ttk.Frame(control_frame)
        row4.pack(fill=tk.X, pady=5)
        ttk.Label(row4, text="价格过滤(最新收盘):").pack(side=tk.LEFT, padx=5)
        ttk.Label(row4, text="最低").pack(side=tk.LEFT, padx=(8, 2))
        self.min_price_var = tk.StringVar(value="")
        min_price_entry = ttk.Entry(row4, textvariable=self.min_price_var, width=8)
        min_price_entry.pack(side=tk.LEFT, padx=(0, 8))
        ttk.Label(row4, text="最高").pack(side=tk.LEFT, padx=(0, 2))
        self.max_price_var = tk.StringVar(value="")
        max_price_entry = ttk.Entry(row4, textvariable=self.max_price_var, width=8)
        max_price_entry.pack(side=tk.LEFT, padx=(0, 10))
        ttk.Button(row4, text="应用价格过滤", command=self.on_price_filter_changed).pack(side=tk.LEFT, padx=4)
        ttk.Button(row4, text="清空价格过滤", command=self.clear_price_filter).pack(side=tk.LEFT, padx=4)
        min_price_entry.bind("<Return>", self.on_price_filter_changed)
        max_price_entry.bind("<Return>", self.on_price_filter_changed)

    def setup_control_panel(self, parent):
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding="10")
        control_frame.pack(fill=tk.X, pady=5)
        self._build_control_scan_params_row(control_frame)
        self._build_control_scan_params_note_row(control_frame)
        self._build_control_actions_row(control_frame)
        self._build_control_board_filter_row(control_frame)
        self._build_control_price_filter_row(control_frame)

    def setup_notebook(self, parent):
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        self.setup_result_tab()
        self.setup_detail_tab()
        self.setup_intraday_tab()
        self.setup_limit_up_compare_tab()
        self.setup_log_tab()

    def setup_result_tab(self):
        result_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(result_frame, text="扫描结果")

        action_frame = ttk.Frame(result_frame)
        action_frame.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_frame, text="导出结果图片", command=self.export_results_image).pack(side=tk.LEFT)
        ttk.Button(action_frame, text="复制代码", command=self.copy_selected_stock_code_name).pack(side=tk.LEFT, padx=8)
        ttk.Label(
            action_frame,
            text="导出图片固定仅包含代码和名称两列，按 Ctrl+C 可复制选中股票代码。",
        ).pack(side=tk.RIGHT)

        self.result_columns = (
            "code",
            "name",
            "board",
            "latest_close",
            "latest_ma",
            "five_day_return",
            "limit_up_streak",
            "broken_limit_up",
            "volume_expand_ratio",
            "volume_expand",
            "volume_break_limit_up",
            "after_two_limit_up",
            "limit_up",
            "recent_closes",
        )
        tree_container = ttk.Frame(result_frame)
        tree_container.pack(fill=tk.BOTH, expand=True)
        tree_container.grid_rowconfigure(0, weight=1)
        tree_container.grid_columnconfigure(0, weight=1)

        self.result_tree = ttk.Treeview(tree_container, columns=self.result_columns, show="headings", height=20)

        self.result_headings = {
            "code": ("代码", 90),
            "name": ("名称", 140),
            "board": ("板块", 120),
            "latest_close": ("最新收盘", 100),
            "latest_ma": ("MA", 100),
            "five_day_return": ("5日涨幅", 90),
            "limit_up_streak": ("连板数", 80),
            "broken_limit_up": ("断板", 70),
            "volume_expand_ratio": ("放量倍数", 90),
            "volume_expand": ("放量", 70),
            "volume_break_limit_up": ("放量断板", 90),
            "after_two_limit_up": ("二连板后", 90),
            "limit_up": ("涨停", 70),
            "recent_closes": ("最近收盘", 220),
        }
        default_visible_columns = (
            "code",
            "name",
            "board",
            "latest_close",
            "latest_ma",
            "five_day_return",
            "limit_up_streak",
            "broken_limit_up",
            "volume_expand_ratio",
            "volume_expand",
            "volume_break_limit_up",
            "after_two_limit_up",
            "limit_up",
        )
        self.default_result_display_columns = default_visible_columns
        self.result_column_order = list(self.result_columns)
        self.result_column_vars = {
            col: tk.BooleanVar(value=(col in default_visible_columns))
            for col in self.result_columns
        }
        for col in self.result_columns:
            text, width = self.result_headings[col]
            self.result_tree.heading(col, text=text, command=lambda c=col: self.on_result_heading_click(c))
            self.result_tree.column(col, width=width, anchor=tk.CENTER)
        self.result_tree.configure(displaycolumns=default_visible_columns)

        scrollbar = ttk.Scrollbar(tree_container, orient=tk.VERTICAL, command=self.result_tree.yview)
        xscrollbar = ttk.Scrollbar(tree_container, orient=tk.HORIZONTAL, command=self.result_tree.xview)
        self.result_tree.configure(yscrollcommand=scrollbar.set)
        self.result_tree.configure(xscrollcommand=xscrollbar.set)

        self.result_tree.grid(row=0, column=0, sticky="nsew")
        scrollbar.grid(row=0, column=1, sticky="ns")
        xscrollbar.grid(row=1, column=0, sticky="ew")

        self.result_tree.bind("<<TreeviewSelect>>", self.on_stock_select)
        self.result_tree.bind("<Double-1>", self.on_stock_double_click)
        self.result_tree.bind("<Control-c>", self.copy_selected_stock_code_name)
        self.result_tree.bind("<Control-C>", self.copy_selected_stock_code_name)

    def _visible_result_columns(self) -> tuple[str, ...]:
        ordered_columns = self.result_column_order or list(self.result_columns)
        visible = tuple(
            col
            for col in ordered_columns
            if self.result_column_vars.get(col) and self.result_column_vars[col].get()
        )
        if visible:
            return visible
        return ("code", "name", "latest_close")

    def _save_board_filter_layout(self) -> None:
        self.board_filter_layout_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "selected": [
                board
                for board, var in self.board_filter_vars.items()
                if var.get()
            ],
        }
        self.board_filter_layout_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_board_filter_layout(self) -> None:
        if not self.board_filter_layout_path.exists() or not self.board_filter_vars:
            return
        try:
            payload = json.loads(self.board_filter_layout_path.read_text(encoding="utf-8"))
        except Exception:
            return
        saved_selected = {
            str(board).strip()
            for board in (payload.get("selected") or [])
            if str(board).strip() in self.board_filter_vars
        }
        if not saved_selected:
            return
        for board, var in self.board_filter_vars.items():
            var.set(board in saved_selected)

    def _save_app_settings(self) -> None:
        self.app_settings_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "history_source": str(self.history_source_var.get() or "auto").strip().lower() or "auto",
            "intraday_source": str(self.intraday_source_var.get() or "auto").strip().lower() or "auto",
            "fund_flow_source": str(self.fund_flow_source_var.get() or "auto").strip().lower() or "auto",
            "limit_up_reason_source": str(self.limit_up_reason_source_var.get() or "auto").strip().lower() or "auto",
        }
        self.app_settings_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_app_settings(self) -> None:
        if not self.app_settings_path.exists():
            return
        try:
            payload = json.loads(self.app_settings_path.read_text(encoding="utf-8"))
        except Exception:
            return
        source = str(payload.get("history_source") or "auto").strip().lower() or "auto"
        if source in DATA_SOURCE_OPTIONS["history"]:
            self.history_source_var.set(source)
        intraday_source = str(payload.get("intraday_source") or "auto").strip().lower() or "auto"
        if intraday_source in DATA_SOURCE_OPTIONS["intraday"]:
            self.intraday_source_var.set(intraday_source)
        fund_flow_source = str(payload.get("fund_flow_source") or "auto").strip().lower() or "auto"
        if fund_flow_source in DATA_SOURCE_OPTIONS["fund_flow"]:
            self.fund_flow_source_var.set(fund_flow_source)
        limit_up_reason_source = str(payload.get("limit_up_reason_source") or "auto").strip().lower() or "auto"
        if limit_up_reason_source in DATA_SOURCE_OPTIONS["limit_up_reason"]:
            self.limit_up_reason_source_var.set(limit_up_reason_source)

    def _apply_source_preferences(self) -> None:
        self.stock_filter.set_history_source_preference(self.history_source_var.get())
        self.stock_filter.set_intraday_source_preference(self.intraday_source_var.get())
        self.stock_filter.set_fund_flow_source_preference(self.fund_flow_source_var.get())
        self.stock_filter.set_limit_up_reason_source_preference(self.limit_up_reason_source_var.get())

    def _save_result_column_layout(self) -> None:
        self.result_layout_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "order": list(self.result_column_order or self.result_columns),
            "visible": list(self._visible_result_columns()),
        }
        self.result_layout_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _load_result_column_layout(self) -> None:
        if not self.result_layout_path.exists() or not self.result_columns:
            return
        try:
            payload = json.loads(self.result_layout_path.read_text(encoding="utf-8"))
        except Exception:
            return

        saved_order = payload.get("order") or []
        normalized_order = [col for col in saved_order if col in self.result_columns]
        for col in self.result_columns:
            if col not in normalized_order:
                normalized_order.append(col)
        if normalized_order:
            self.result_column_order = normalized_order

        saved_visible = set(payload.get("visible") or [])
        if saved_visible:
            for col, var in self.result_column_vars.items():
                var.set(col in saved_visible)

    def reset_result_columns(self) -> None:
        self.result_column_order = list(self.result_columns)
        visible = set(self.default_result_display_columns)
        for col, var in self.result_column_vars.items():
            var.set(col in visible)
        self.apply_result_display_columns()

    def apply_result_display_columns(self, save: bool = True) -> None:
        if not hasattr(self, "result_tree"):
            return
        self.result_tree.configure(displaycolumns=self._visible_result_columns())
        if save:
            self._save_result_column_layout()

    def _get_result_display_columns_and_headings(self) -> List[tuple[str, str]]:
        return [
            (col, self.result_headings.get(col, (col, 100))[0])
            for col in self._visible_result_columns()
        ]

    def _format_result_row_values(self, result: Dict[str, Any]) -> Dict[str, str]:
        data = result.get("data", {}) or {}
        analysis = data.get("analysis") or {}
        recent = analysis.get("recent_closes") or []
        five_day_return = analysis.get("five_day_return")
        volume_expand_ratio = analysis.get("volume_expand_ratio")

        return {
            "code": str(result.get("code", "-") or "-"),
            "name": str(result.get("name", "-") or "-"),
            "board": str(data.get("board") or data.get("exchange") or "-"),
            "latest_close": "-" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}",
            "latest_ma": "-" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}",
            "five_day_return": "-" if five_day_return is None else f"{five_day_return:.2f}%",
            "limit_up_streak": str(analysis.get("limit_up_streak") or 0),
            "broken_limit_up": "是" if analysis.get("broken_limit_up") else "否",
            "volume_expand_ratio": "-" if volume_expand_ratio is None else f"{volume_expand_ratio:.2f}x",
            "volume_expand": "是" if analysis.get("volume_expand") else "否",
            "volume_break_limit_up": "是" if analysis.get("volume_break_limit_up") else "否",
            "after_two_limit_up": "是" if analysis.get("after_two_limit_up") else "否",
            "limit_up": "是" if analysis.get("limit_up") else "否",
            "recent_closes": ", ".join("-" if v is None else f"{v:.2f}" for v in recent),
        }

    def _build_result_image_pages(self, rows: List[List[str]], page_size: int = 40) -> List[List[List[str]]]:
        if not rows:
            return []
        return [rows[index : index + page_size] for index in range(0, len(rows), page_size)]

    def _get_selected_result_identity(self) -> Optional[tuple[str, str]]:
        selection = self.result_tree.selection()
        if not selection:
            return None
        item = self.result_tree.item(selection[0])
        values = item.get("values") or []
        if len(values) < 2:
            return None
        stock_code = str(values[0]).strip().zfill(6)
        stock_name = str(values[1]).strip()
        if not stock_code or not stock_name:
            return None
        return stock_code, stock_name

    def show_column_picker(self) -> None:
        picker = tk.Toplevel(self.root)
        picker.title("列表列设置")
        picker.geometry("520x460")
        picker.transient(self.root)
        picker.grab_set()

        frame = ttk.Frame(picker, padding="16")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="可调整列顺序和显示状态，设置会自动保存。").pack(anchor=tk.W, pady=(0, 10))

        body = ttk.Frame(frame)
        body.pack(fill=tk.BOTH, expand=True)

        list_frame = ttk.Frame(body)
        list_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        column_listbox = tk.Listbox(list_frame, height=14, activestyle="dotbox")
        column_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        list_scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=column_listbox.yview)
        list_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        column_listbox.configure(yscrollcommand=list_scrollbar.set)

        button_frame = ttk.Frame(body)
        button_frame.pack(side=tk.LEFT, fill=tk.Y, padx=(12, 0))

        def refresh_column_listbox(keep_selection: Optional[int] = None) -> None:
            column_listbox.delete(0, tk.END)
            for col in self.result_column_order:
                label, _ = self.result_headings[col]
                flag = "显示" if self.result_column_vars[col].get() else "隐藏"
                column_listbox.insert(tk.END, f"[{flag}] {label}")
            if keep_selection is None and self.result_column_order:
                keep_selection = 0
            if keep_selection is not None and self.result_column_order:
                keep_selection = max(0, min(keep_selection, len(self.result_column_order) - 1))
                column_listbox.selection_clear(0, tk.END)
                column_listbox.selection_set(keep_selection)
                column_listbox.activate(keep_selection)

        def selected_index() -> Optional[int]:
            selection = column_listbox.curselection()
            if not selection:
                return None
            return int(selection[0])

        def move_selected(offset: int) -> None:
            index = selected_index()
            if index is None:
                return
            new_index = index + offset
            if new_index < 0 or new_index >= len(self.result_column_order):
                return
            self.result_column_order[index], self.result_column_order[new_index] = (
                self.result_column_order[new_index],
                self.result_column_order[index],
            )
            self.apply_result_display_columns()
            refresh_column_listbox(new_index)

        def toggle_selected() -> None:
            index = selected_index()
            if index is None:
                return
            col = self.result_column_order[index]
            self.result_column_vars[col].set(not self.result_column_vars[col].get())
            self.apply_result_display_columns()
            refresh_column_listbox(index)

        def show_all() -> None:
            for var in self.result_column_vars.values():
                var.set(True)
            self.apply_result_display_columns()
            refresh_column_listbox(selected_index())

        def show_core() -> None:
            core = {
                "code",
                "name",
                "board",
                "latest_close",
                "latest_ma",
                "five_day_return",
                "limit_up_streak",
                "broken_limit_up",
                "volume_expand_ratio",
                "volume_expand",
                "volume_break_limit_up",
                "after_two_limit_up",
                "limit_up",
            }
            for col, var in self.result_column_vars.items():
                var.set(col in core)
            self.apply_result_display_columns()
            refresh_column_listbox(selected_index())

        def reset_columns() -> None:
            self.reset_result_columns()
            refresh_column_listbox(0)

        ttk.Button(button_frame, text="上移", command=lambda: move_selected(-1)).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="下移", command=lambda: move_selected(1)).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="显示/隐藏", command=toggle_selected).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="显示核心列", command=show_core).pack(fill=tk.X, pady=(16, 4))
        ttk.Button(button_frame, text="显示全部列", command=show_all).pack(fill=tk.X, pady=4)
        ttk.Button(button_frame, text="重置列", command=reset_columns).pack(fill=tk.X, pady=4)

        action_row = ttk.Frame(frame)
        action_row.pack(fill=tk.X, pady=(12, 0))
        ttk.Button(action_row, text="关闭", command=picker.destroy).pack(side=tk.RIGHT)

        refresh_column_listbox(0)

    def setup_detail_tab(self):
        detail_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(detail_frame, text="股票详情")
        self.detail_tab_frame = detail_frame

        info_header = ttk.Frame(detail_frame)
        info_header.pack(fill=tk.X, pady=(5, 2))
        self.detail_info_header = info_header
        self.detail_summary_toggle_btn = ttk.Button(
            info_header,
            text="展开历史摘要",
            command=self.toggle_detail_summary_section,
        )
        self.detail_summary_toggle_btn.pack(side=tk.LEFT)
        self.detail_summary_status_var = tk.StringVar(value="历史摘要已收起")
        ttk.Label(info_header, textvariable=self.detail_summary_status_var).pack(side=tk.LEFT, padx=10)

        self.info_frame = ttk.LabelFrame(detail_frame, text="历史摘要", padding="10")
        self.info_frame.pack(fill=tk.X, pady=5)

        self.detail_labels: Dict[str, ttk.Label] = {}
        self.detail_label_caption_vars: Dict[str, tk.StringVar] = {}
        items = [
            ("code", "股票代码"),
            ("name", "股票名称"),
            ("latest_date", "最新日期"),
            ("quote_time", "刷新时间"),
            ("latest_close", "最新收盘"),
            ("latest_ma", f"MA{max(1, int(self.stock_filter.ma_period))}"),
            ("latest_ma10", "MA10"),
            ("latest_volume", "成交量"),
            ("latest_amount", "成交额"),
            ("five_day_return", "5日涨幅"),
            ("limit_up", "涨停"),
            ("volume_expand", "放量"),
            ("volume_expand_ratio", "放量倍数"),
            ("big_order_amount", "大单净额"),
            ("main_force_amount", "主力净额"),
            ("summary", "结论"),
        ]

        for i, (key, label) in enumerate(items):
            row = i // 3
            col = (i % 3) * 2
            caption_var = tk.StringVar(value=f"{label}:")
            self.detail_label_caption_vars[key] = caption_var
            ttk.Label(self.info_frame, textvariable=caption_var).grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.detail_labels[key] = ttk.Label(self.info_frame, text="-", width=30)
            self.detail_labels[key].grid(row=row, column=col + 1, padx=5, pady=5, sticky=tk.W)

        self.info_frame.pack_forget()

        chart_header = ttk.Frame(detail_frame)
        chart_header.pack(fill=tk.X, pady=(6, 2))
        self.detail_chart_header = chart_header
        self.detail_chart_toggle_btn = ttk.Button(
            chart_header,
            text="收起历史K线",
            command=self.toggle_detail_chart_section,
        )
        self.detail_chart_toggle_btn.pack(side=tk.LEFT)
        self.detail_chart_status_var = tk.StringVar(value="历史K线已展开")
        ttk.Label(chart_header, textvariable=self.detail_chart_status_var).pack(side=tk.LEFT, padx=10)

        self.detail_flow_toggle_btn = ttk.Button(
            chart_header,
            text="展开大单净额",
            command=self.toggle_detail_flow_section,
        )
        self.detail_flow_toggle_btn.pack(side=tk.RIGHT)
        self.detail_flow_status_var = tk.StringVar(value="大单净额已收起")
        ttk.Label(chart_header, textvariable=self.detail_flow_status_var).pack(side=tk.RIGHT, padx=10)

        self.chart_frame = ttk.LabelFrame(detail_frame, text="K线图", padding="5")
        self.chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.chart_body = ttk.Frame(self.chart_frame)
        self.chart_body.pack(fill=tk.BOTH, expand=True)

        self.fig, (self.price_ax, self.volume_ax, self.flow_ax) = plt.subplots(
            3,
            1,
            figsize=(12.8, 10.0),
            sharex=True,
            gridspec_kw={"height_ratios": [4.8, 1.25, 1.25]},
        )
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_body)
        canvas_widget = self.canvas.get_tk_widget()
        canvas_widget.pack(fill=tk.BOTH, expand=True)
        self.canvas.mpl_connect("button_press_event", self.on_detail_chart_click)
        self.canvas.mpl_connect("scroll_event", self.on_detail_chart_scroll)
        self.canvas.mpl_connect("motion_notify_event", self.on_detail_chart_drag_motion)
        self.canvas.mpl_connect("button_release_event", self.on_detail_chart_drag_release)
        self._bind_detail_chart_scroll(canvas_widget)

        slider_row = ttk.Frame(self.chart_body)
        slider_row.pack(fill=tk.X, pady=(6, 0))
        ttk.Label(slider_row, text="左右滑动").pack(side=tk.LEFT)
        self.detail_chart_window_var = tk.DoubleVar(value=0.0)
        self.detail_chart_window_scale = tk.Scale(
            slider_row,
            orient=tk.HORIZONTAL,
            from_=0,
            to=0,
            resolution=1,
            showvalue=False,
            variable=self.detail_chart_window_var,
            command=self.on_detail_chart_window_changed,
            length=480,
        )
        self.detail_chart_window_scale.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=8)
        self.detail_chart_window_label_var = tk.StringVar(value="窗口: -")
        ttk.Label(slider_row, textvariable=self.detail_chart_window_label_var).pack(side=tk.RIGHT)

        self.detail_chart_placeholder = None
        self._apply_detail_section_visibility()

    def setup_intraday_tab(self):
        intraday_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(intraday_frame, text="分时")
        self.intraday_tab = intraday_frame

        info = ttk.Frame(intraday_frame)
        info.pack(fill=tk.X, pady=(0, 6))
        self.intraday_title_var = tk.StringVar(value="分时图（点击 K 线打开）")
        ttk.Label(info, textvariable=self.intraday_title_var).pack(side=tk.LEFT)
        self.intraday_day_var = tk.StringVar(value="交易日: -")
        ttk.Label(info, textvariable=self.intraday_day_var).pack(side=tk.LEFT, padx=(12, 8))
        self.intraday_prev_btn = ttk.Button(info, text="前一天", command=lambda: self.navigate_intraday_day(-1), state=tk.DISABLED)
        self.intraday_prev_btn.pack(side=tk.RIGHT, padx=(6, 0))
        self.intraday_next_btn = ttk.Button(info, text="后一天", command=lambda: self.navigate_intraday_day(1), state=tk.DISABLED)
        self.intraday_next_btn.pack(side=tk.RIGHT)

        chart_frame = ttk.LabelFrame(intraday_frame, text="分时走势", padding="5")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.intraday_fig = Figure(figsize=(11, 6.8), dpi=100)
        gs = self.intraday_fig.add_gridspec(
            2,
            2,
            width_ratios=[4.0, 1.4],
            height_ratios=[3.0, 1.2],
            wspace=0.32,
            hspace=0.14,
        )
        self.intraday_price_ax = self.intraday_fig.add_subplot(gs[0, 0])
        self.intraday_volume_ax = self.intraday_fig.add_subplot(gs[1, 0], sharex=self.intraday_price_ax)
        self.intraday_dist_ax = self.intraday_fig.add_subplot(gs[:, 1])
        self.intraday_canvas = FigureCanvasTkAgg(self.intraday_fig, master=chart_frame)
        self.intraday_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self._draw_intraday_loading("点击详情页 K 线打开分时")

    # ================= 涨停对比 Tab =================
    def setup_limit_up_compare_tab(self):
        compare_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(compare_frame, text="涨停对比")

        # ---- 操作栏 ----
        action_bar = ttk.Frame(compare_frame)
        action_bar.pack(fill=tk.X, pady=(0, 6))
        ttk.Button(action_bar, text="获取涨停对比", command=self._start_limit_up_compare).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="今日:").pack(side=tk.LEFT, padx=(12, 2))
        self._zt_today_var = tk.StringVar(value=datetime.now().strftime("%Y%m%d"))
        ttk.Entry(action_bar, textvariable=self._zt_today_var, width=10).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="昨日:").pack(side=tk.LEFT, padx=(8, 2))
        self._zt_yesterday_var = tk.StringVar()
        ttk.Entry(action_bar, textvariable=self._zt_yesterday_var, width=10).pack(side=tk.LEFT)
        ttk.Label(action_bar, text="(昨日留空自动推断)").pack(side=tk.LEFT, padx=4)
        self._zt_status_label = ttk.Label(action_bar, text="")
        self._zt_status_label.pack(side=tk.RIGHT, padx=8)

        # ---- 主区域：左侧摘要 + 右侧表格 ----
        body = ttk.PanedWindow(compare_frame, orient=tk.HORIZONTAL)
        body.pack(fill=tk.BOTH, expand=True)

        # 左侧：摘要面板
        summary_frame = ttk.LabelFrame(body, text="对比摘要 + 形态分布", padding="6")
        self._zt_summary_text = scrolledtext.ScrolledText(summary_frame, width=42, height=30, wrap=tk.WORD)
        self._zt_summary_text.pack(fill=tk.BOTH, expand=True)
        self._zt_summary_text.insert(tk.END, "点击「获取涨停对比」开始分析\n\n"
            "分析每只涨停股的技术形态:\n"
            "  - 回踩MA5涨停: 前日触及五日线后反弹\n"
            "  - 超跌反弹涨停: 下跌趋势中突然涨停\n"
            "  - 趋势加速涨停: 均线多头中涨停加速\n"
            "  - 高位连板: 连续涨停\n"
            "  - 突破平台涨停: 横盘整理后突破\n"
            "  - 首板低位涨停: 低位首次涨停\n")
        self._zt_summary_text.config(state=tk.DISABLED)
        body.add(summary_frame, weight=2)

        # 右侧：表格区
        table_frame = ttk.Frame(body)
        self._zt_table_nb = ttk.Notebook(table_frame)
        self._zt_table_nb.pack(fill=tk.BOTH, expand=True)

        # 今日涨停形态分类 Tab（核心表格）
        pattern_tab = ttk.Frame(self._zt_table_nb)
        self._zt_table_nb.add(pattern_tab, text="今日涨停形态分类")
        zt_pattern_cols = ("code", "name", "industry", "pattern", "change_pct", "close",
                           "dist_ma5", "trend_10d", "pos_60d", "detail")
        self._zt_pattern_tree = ttk.Treeview(pattern_tab, columns=zt_pattern_cols, show="headings", height=22)
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "pattern": ("技术形态", 110), "change_pct": ("涨跌幅%", 70), "close": ("最新价", 70),
            "dist_ma5": ("距MA5%", 70), "trend_10d": ("10日涨幅%", 80),
            "pos_60d": ("60日分位%", 80), "detail": ("形态说明", 220),
        }.items():
            self._zt_pattern_tree.heading(col, text=heading)
            self._zt_pattern_tree.column(col, width=w, anchor=tk.CENTER if col != "detail" else tk.W)
        sb_p = ttk.Scrollbar(pattern_tab, orient=tk.VERTICAL, command=self._zt_pattern_tree.yview)
        self._zt_pattern_tree.configure(yscrollcommand=sb_p.set)
        sb_p.pack(side=tk.RIGHT, fill=tk.Y)
        self._zt_pattern_tree.pack(fill=tk.BOTH, expand=True)
        # 行标签色
        self._zt_pattern_tree.tag_configure("pat_ma5", background="#e8f5e9")
        self._zt_pattern_tree.tag_configure("pat_oversold", background="#fff3e0")
        self._zt_pattern_tree.tag_configure("pat_trend", background="#e3f2fd")
        self._zt_pattern_tree.tag_configure("pat_streak", background="#fce4ec")
        self._zt_pattern_tree.tag_configure("pat_breakout", background="#f3e5f5")
        self._zt_pattern_tree.tag_configure("pat_lowpos", background="#e0f7fa")

        # 昨日首板今日表现 Tab
        yest_tab = ttk.Frame(self._zt_table_nb)
        self._zt_table_nb.add(yest_tab, text="昨日首板今日表现")
        zt_cols_yest = ("code", "name", "industry", "pattern", "today_chg", "close", "still_zt", "status")
        self._zt_yest_tree = ttk.Treeview(yest_tab, columns=zt_cols_yest, show="headings", height=22)
        for col, (heading, w) in {
            "code": ("代码", 70), "name": ("名称", 85), "industry": ("行业", 85),
            "pattern": ("昨日形态", 110), "today_chg": ("今日涨跌%", 80), "close": ("最新价", 70),
            "still_zt": ("继续涨停", 70), "status": ("状态", 70),
        }.items():
            self._zt_yest_tree.heading(col, text=heading)
            self._zt_yest_tree.column(col, width=w, anchor=tk.CENTER)
        sb2 = ttk.Scrollbar(yest_tab, orient=tk.VERTICAL, command=self._zt_yest_tree.yview)
        self._zt_yest_tree.configure(yscrollcommand=sb2.set)
        sb2.pack(side=tk.RIGHT, fill=tk.Y)
        self._zt_yest_tree.pack(fill=tk.BOTH, expand=True)

        body.add(table_frame, weight=4)

        self._zt_compare_thread = None
        self._zt_compare_result: Optional[Dict[str, Any]] = None

    _ZT_PATTERN_TAG_MAP = {
        "回踩MA5涨停": "pat_ma5",
        "超跌反弹涨停": "pat_oversold",
        "趋势加速涨停": "pat_trend",
        "高位连板": "pat_streak",
        "突破平台涨停": "pat_breakout",
        "首板低位涨停": "pat_lowpos",
    }

    def _estimate_yesterday(self, today_str: str) -> str:
        from datetime import timedelta
        try:
            d = datetime.strptime(today_str.strip(), "%Y%m%d").date()
        except (ValueError, TypeError):
            d = datetime.now().date()
        d -= timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        return d.strftime("%Y%m%d")

    def _start_limit_up_compare(self):
        if self._zt_compare_thread is not None and self._zt_compare_thread.is_alive():
            return
        today = self._zt_today_var.get().strip()
        if not today:
            today = datetime.now().strftime("%Y%m%d")
            self._zt_today_var.set(today)
        yesterday = self._zt_yesterday_var.get().strip()
        if not yesterday:
            yesterday = self._estimate_yesterday(today)
            self._zt_yesterday_var.set(yesterday)

        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.delete("1.0", tk.END)
        self._zt_summary_text.insert(tk.END, f"正在获取 {today} vs {yesterday} 涨停数据...\n")
        self._zt_summary_text.config(state=tk.DISABLED)
        self._zt_status_label.config(text="获取涨停池...")
        self.status_var.set("正在获取涨停对比...")

        self._zt_compare_thread = threading.Thread(
            target=self._load_limit_up_compare, args=(today, yesterday), daemon=True
        )
        self._zt_compare_thread.start()

    def _load_limit_up_compare(self, today: str, yesterday: str):
        try:
            # 阶段1：获取涨停池对比数据
            result = self.stock_filter.fetcher.compare_limit_up_pools(today, yesterday)
            self.root.after(0, lambda: self._zt_status_label.config(
                text=f"涨停池获取完成，正在分析今日 {len(result.get('today_first', []))} 只首板形态..."
            ))

            # 阶段2：对今日首板做技术形态分类
            today_first = result.get("today_first", [])
            def _progress(cur, tot, info):
                self.root.after(0, lambda c=cur, t=tot, i=info:
                    self._zt_status_label.config(text=f"分类今日首板 {c}/{t}: {i}"))
            today_classified = self.stock_filter.classify_limit_up_pool(today_first, progress_callback=_progress)
            result["today_classified"] = today_classified

            # 阶段3：对昨日首板做技术形态分类
            yesterday_first = result.get("yesterday_first", [])
            if yesterday_first:
                self.root.after(0, lambda: self._zt_status_label.config(
                    text=f"正在分析昨日 {len(yesterday_first)} 只首板形态..."))
                def _progress2(cur, tot, info):
                    self.root.after(0, lambda c=cur, t=tot, i=info:
                        self._zt_status_label.config(text=f"分类昨日首板 {c}/{t}: {i}"))
                yesterday_classified = self.stock_filter.classify_limit_up_pool(yesterday_first, progress_callback=_progress2)
                result["yesterday_classified"] = yesterday_classified

            self.root.after(0, lambda r=result: self._apply_limit_up_compare(r))
        except Exception as e:
            err = str(e)
            self.root.after(0, lambda: self._zt_show_error(f"涨停对比失败: {err}"))

    def _zt_show_error(self, msg: str):
        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.delete("1.0", tk.END)
        self._zt_summary_text.insert(tk.END, msg)
        self._zt_summary_text.config(state=tk.DISABLED)
        self._zt_status_label.config(text="")
        self.status_var.set("涨停对比失败")

    def _apply_limit_up_compare(self, result: Dict[str, Any]):
        self._zt_compare_result = result
        today_classified = result.get("today_classified", [])
        yesterday_classified = result.get("yesterday_classified", [])

        # ---- 统计形态分布 ----
        pattern_counts_today: Dict[str, int] = {}
        for rec in today_classified:
            p = rec.get("pattern", "其他涨停")
            pattern_counts_today[p] = pattern_counts_today.get(p, 0) + 1

        pattern_counts_yest: Dict[str, int] = {}
        for rec in yesterday_classified:
            p = rec.get("pattern", "其他涨停")
            pattern_counts_yest[p] = pattern_counts_yest.get(p, 0) + 1

        # ---- 填充摘要 ----
        self._zt_summary_text.config(state=tk.NORMAL)
        self._zt_summary_text.delete("1.0", tk.END)
        txt = self._zt_summary_text

        txt.insert(tk.END, result.get("summary", "") + "\n")

        txt.insert(tk.END, f"\n{'='*36}\n")
        txt.insert(tk.END, f"  今日首板形态分布 ({len(today_classified)} 只)\n")
        txt.insert(tk.END, f"{'='*36}\n")
        for p, c in sorted(pattern_counts_today.items(), key=lambda x: -x[1]):
            pct = c / max(len(today_classified), 1) * 100
            bar = "#" * int(pct / 3)
            txt.insert(tk.END, f"  {p:10s}  {c:3d} 只  {pct:5.1f}%  {bar}\n")

        if pattern_counts_yest:
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, f"  昨日首板形态分布 ({len(yesterday_classified)} 只)\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for p, c in sorted(pattern_counts_yest.items(), key=lambda x: -x[1]):
                pct = c / max(len(yesterday_classified), 1) * 100
                bar = "#" * int(pct / 3)
                txt.insert(tk.END, f"  {p:10s}  {c:3d} 只  {pct:5.1f}%  {bar}\n")

            # 形态变化对比
            all_patterns = sorted(set(list(pattern_counts_today.keys()) + list(pattern_counts_yest.keys())))
            txt.insert(tk.END, f"\n{'='*36}\n")
            txt.insert(tk.END, "  形态变化（今日 vs 昨日）\n")
            txt.insert(tk.END, f"{'='*36}\n")
            for p in all_patterns:
                t = pattern_counts_today.get(p, 0)
                y = pattern_counts_yest.get(p, 0)
                delta = t - y
                arrow = "+" if delta > 0 else ""
                txt.insert(tk.END, f"  {p:10s}  {y:2d} → {t:2d}  ({arrow}{delta})\n")

        # 行业分布
        ind_today = result.get("industry_today", {})
        ind_yest = result.get("industry_yesterday", {})
        if ind_today:
            txt.insert(tk.END, "\n── 今日首板 TOP 行业 ──\n")
            for k, v in sorted(ind_today.items(), key=lambda x: -x[1])[:8]:
                txt.insert(tk.END, f"  {k}: {v} 只\n")
        if ind_yest:
            txt.insert(tk.END, "\n── 昨日首板 TOP 行业 ──\n")
            for k, v in sorted(ind_yest.items(), key=lambda x: -x[1])[:8]:
                txt.insert(tk.END, f"  {k}: {v} 只\n")

        continued = result.get("continued_codes", [])
        lost = result.get("lost_codes", [])
        if continued:
            txt.insert(tk.END, f"\n── 昨日首板→今日继续涨停 ({len(continued)}) ──\n")
            txt.insert(tk.END, "  " + ", ".join(continued) + "\n")
        if lost:
            txt.insert(tk.END, f"\n── 昨日首板→今日未涨停 ({len(lost)}) ──\n")
            txt.insert(tk.END, "  " + ", ".join(lost) + "\n")
        self._zt_summary_text.config(state=tk.DISABLED)

        # ---- 填充今日涨停形态分类表格 ----
        self._zt_pattern_tree.delete(*self._zt_pattern_tree.get_children())
        for rec in sorted(today_classified, key=lambda r: r.get("pattern", "")):
            tag = self._ZT_PATTERN_TAG_MAP.get(rec.get("pattern", ""), "")
            vals = (
                rec.get("code", ""),
                rec.get("name", ""),
                rec.get("industry", ""),
                rec.get("pattern", ""),
                f"{rec['change_pct']:.2f}" if rec.get("change_pct") is not None else "-",
                f"{rec['close']:.2f}" if rec.get("close") is not None else "-",
                f"{rec['distance_ma5_pct']:+.1f}" if rec.get("distance_ma5_pct") is not None else "-",
                f"{rec['trend_10d_pct']:+.1f}" if rec.get("trend_10d_pct") is not None else "-",
                f"{rec['position_60d_pct']:.0f}" if rec.get("position_60d_pct") is not None else "-",
                rec.get("pattern_detail", ""),
            )
            self._zt_pattern_tree.insert("", tk.END, values=vals, tags=(tag,) if tag else ())

        # ---- 填充昨日首板今日表现表格（带形态） ----
        self._zt_yest_tree.delete(*self._zt_yest_tree.get_children())
        yest_class_map = {r["code"]: r for r in yesterday_classified}
        perf_map = {p["code"]: p for p in result.get("yesterday_first_today_performance", [])}
        for rec in yesterday_classified:
            code = rec.get("code", "")
            perf = perf_map.get(code, {})
            chg = perf.get("change_pct")
            close_val = perf.get("close")
            still = perf.get("still_limit_up", False)
            if still:
                status = "晋级"
            elif chg is not None and chg > 0:
                status = "高开"
            elif chg is not None and chg < -3:
                status = "大跌"
            elif chg is not None:
                status = "平开" if chg >= -1 else "低开"
            else:
                status = "-"
            vals = (
                code,
                rec.get("name", ""),
                rec.get("industry", ""),
                rec.get("pattern", ""),
                f"{chg:.2f}" if chg is not None else "-",
                f"{close_val:.2f}" if close_val is not None else "-",
                "是" if still else "否",
                status,
            )
            self._zt_yest_tree.insert("", tk.END, values=vals)

        self._zt_status_label.config(text="")
        self.status_var.set("涨停对比完成")

    def setup_log_tab(self):
        log_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(log_frame, text="运行日志")

        self.log_text = scrolledtext.ScrolledText(log_frame, height=30, width=100)
        self.log_text.pack(fill=tk.BOTH, expand=True)

    def setup_status_bar(self):
        self.status_var = tk.StringVar(value="就绪")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(self.root, variable=self.progress_var, maximum=100)
        self.progress_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def _open_run_log(self) -> None:
        log_dir = Path("data") / "run_logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._run_log_file = log_dir / f"scan_{stamp}.log"
        self._run_log_file.write_text("", encoding="utf-8")

    def _close_run_log(self) -> None:
        self._run_log_file = None

    def _log(self, message: str) -> None:
        if self._is_closing:
            return
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
        self.log_text.insert(tk.END, line)
        self.log_text.see(tk.END)
        if self._run_log_file is not None:
            with self._run_log_file.open("a", encoding="utf-8") as f:
                f.write(line)

    def _log_async(self, message: str) -> None:
        if self._is_closing:
            return
        if threading.get_ident() == self._main_thread_id:
            self._log(message)
            return
        self._log_queue.put(message)

    def _drain_log_queue(self) -> None:
        if self._is_closing or not self.root.winfo_exists():
            return
        while True:
            try:
                message = self._log_queue.get_nowait()
            except queue.Empty:
                break
            self._log(message)
        if not self._is_closing and self.root.winfo_exists():
            self.root.after(100, self._drain_log_queue)

    def _safe_after(self, delay_ms: int, callback) -> None:
        if self._is_closing:
            return
        try:
            if self.root.winfo_exists():
                self.root.after(delay_ms, callback)
        except tk.TclError:
            pass

    def _selected_boards(self) -> List[str]:
        boards = [board for board, var in self.board_filter_vars.items() if var.get()]
        return boards or list(self.board_filter_vars.keys())

    def _parse_int_value(
        self,
        raw_value: str,
        field_name: str,
        minimum: int,
        maximum: Optional[int] = None,
        allow_zero: bool = False,
    ) -> int:
        text = str(raw_value).strip()
        try:
            value = int(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是整数") from exc
        if allow_zero and value == 0:
            return 0
        if value < minimum:
            raise ValueError(f"{field_name} 不能小于 {minimum}")
        if maximum is not None and value > maximum:
            raise ValueError(f"{field_name} 不能大于 {maximum}")
        return value

    def _parse_float_value(
        self,
        raw_value: str,
        field_name: str,
        minimum: float,
        maximum: Optional[float] = None,
    ) -> float:
        text = str(raw_value).strip()
        try:
            value = float(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是数字") from exc
        if value < minimum:
            raise ValueError(f"{field_name} 不能小于 {minimum:g}")
        if maximum is not None and value > maximum:
            raise ValueError(f"{field_name} 不能大于 {maximum:g}")
        return value

    def _parse_optional_price_limit(self, raw_value: str, field_name: str) -> Optional[float]:
        text = str(raw_value).strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError as exc:
            raise ValueError(f"{field_name} 必须是数字") from exc
        if value < 0:
            raise ValueError(f"{field_name} 不能小于 0")
        return value

    def _build_filter_settings(self) -> FilterSettings:
        return FilterSettings(
            trend_days=self._parse_int_value(self.trend_days_var.get(), "连续天数", minimum=1, maximum=120),
            ma_period=self._parse_int_value(self.ma_period_var.get(), "MA周期", minimum=1, maximum=250),
            limit_up_lookback_days=self._parse_int_value(
                self.limit_up_lookback_var.get(),
                "近N日涨停",
                minimum=1,
                maximum=60,
            ),
            volume_lookback_days=self._parse_int_value(
                self.volume_lookback_var.get(),
                "放量观察天数",
                minimum=1,
                maximum=60,
            ),
            volume_expand_enabled=bool(self.volume_expand_enabled_var.get()),
            volume_expand_factor=self._parse_float_value(
                self.volume_expand_factor_var.get(),
                "放量倍数阈值",
                minimum=1.0,
                maximum=50.0,
            ),
            require_limit_up_within_days=bool(self.require_limit_up_var.get()),
        )

    def _apply_filter_settings_from_ui(self, show_error: bool = True) -> Optional[FilterSettings]:
        try:
            settings = self._build_filter_settings()
        except ValueError as exc:
            if show_error:
                messagebox.showerror("错误", str(exc))
            return None
        self.stock_filter.apply_settings(settings)
        self._refresh_detail_metric_labels()
        return settings

    def _build_scan_request(self) -> Optional[ScanRequest]:
        settings = self._apply_filter_settings_from_ui(show_error=True)
        if settings is None:
            return None
        try:
            max_stocks = self._parse_int_value(
                self.scan_count_var.get(),
                "扫描数量",
                minimum=1,
                maximum=10000,
                allow_zero=True,
            )
            scan_workers = self._parse_int_value(
                self.scan_workers_var.get(),
                "并发线程",
                minimum=1,
                maximum=16,
            )
        except ValueError as exc:
            messagebox.showerror("错误", str(exc))
            return None
        return ScanRequest(
            filter_settings=settings,
            max_stocks=max_stocks,
            scan_workers=scan_workers,
            history_source=str(self.history_source_var.get() or "auto").strip().lower() or "auto",
            allowed_boards=tuple(self._selected_boards()),
            refresh_universe=bool(self.refresh_universe_var.get()),
            ignore_result_snapshot=bool(self.ignore_result_snapshot_var.get()),
        )

    def _short_text(self, value: Any, max_len: int = 28) -> str:
        text = str(value or "").strip()
        if not text or text == "-":
            return "-"
        if len(text) <= max_len:
            return text
        return f"{text[: max_len - 1]}…"

    def _format_amount(self, value: Any) -> str:
        try:
            if value is None or value == "":
                return "-"
            amount = float(value)
        except (TypeError, ValueError):
            return "-"
        abs_amount = abs(amount)
        if abs_amount >= 1e8:
            return f"{amount / 1e8:.2f}亿"
        if abs_amount >= 1e4:
            return f"{amount / 1e4:.2f}万"
        return f"{amount:.0f}"

    def _format_volume(self, value: Any) -> str:
        try:
            if value is None or value == "":
                return "-"
            volume = float(value)
        except (TypeError, ValueError):
            return "-"
        abs_volume = abs(volume)
        if abs_volume >= 1e8:
            return f"{volume / 1e8:.2f}亿"
        if abs_volume >= 1e4:
            return f"{volume / 1e4:.2f}万"
        return f"{volume:.0f}"

    def _format_axis_volume(self, value: float, _pos: float = 0) -> str:
        text = self._format_volume(value)
        return text if text != "-" else "0"

    def _refresh_detail_metric_labels(self) -> None:
        ma_period = max(1, int(self.stock_filter.ma_period))
        if "latest_ma" in self.detail_label_caption_vars:
            self.detail_label_caption_vars["latest_ma"].set(f"MA{ma_period}:")
        volume_days = max(1, int(self.stock_filter.volume_lookback_days))
        if "latest_volume" in self.detail_label_caption_vars:
            self.detail_label_caption_vars["latest_volume"].set(f"成交量(近{volume_days}日均量占比):")

    def _filter_results_by_selected_boards(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        allowed = {str(board).strip() for board in self._selected_boards() if str(board).strip()}
        if not allowed:
            return list(results)
        filtered: List[Dict[str, Any]] = []
        for item in results:
            data = item.get("data", {}) or {}
            code = str(item.get("code", "")).strip().zfill(6)
            board = str(data.get("board") or "").strip()
            if not board:
                if code.startswith(("300", "301")):
                    board = "创业板"
                elif code.startswith("688"):
                    board = "科创板"
                elif code.startswith(("000", "001", "002", "003")):
                    board = "深交所主板"
                elif code.startswith(("5", "6", "9")):
                    board = "上交所主板"
                else:
                    board = str(data.get("exchange") or "").strip()
            if board in allowed:
                filtered.append(item)
        return filtered

    def _get_latest_close_value(self, item: Dict[str, Any]) -> Optional[float]:
        data = item.get("data", {}) or {}
        analysis = data.get("analysis") or {}
        value = analysis.get("latest_close")
        if value in (None, ""):
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _filter_results_by_price_range(
        self,
        results: List[Dict[str, Any]],
        raise_error: bool = False,
    ) -> List[Dict[str, Any]]:
        try:
            min_price = self._parse_optional_price_limit(self.min_price_var.get(), "最低价")
            max_price = self._parse_optional_price_limit(self.max_price_var.get(), "最高价")
            if min_price is not None and max_price is not None and min_price > max_price:
                raise ValueError("最低价不能大于最高价")
        except ValueError:
            if raise_error:
                raise
            return list(results)
        if min_price is None and max_price is None:
            return list(results)

        filtered: List[Dict[str, Any]] = []
        for item in results:
            latest_close = self._get_latest_close_value(item)
            if latest_close is None:
                continue
            if min_price is not None and latest_close < min_price:
                continue
            if max_price is not None and latest_close > max_price:
                continue
            filtered.append(item)
        return filtered

    def _apply_result_filters(self, results: List[Dict[str, Any]], raise_price_error: bool = False) -> List[Dict[str, Any]]:
        filtered = self._filter_results_by_selected_boards(results)
        filtered = self._filter_results_by_price_range(filtered, raise_error=raise_price_error)
        return filtered

    def on_board_filter_changed(self):
        self._save_board_filter_layout()
        if self.is_scanning:
            return
        source = self.all_scan_results or self.filtered_stocks
        if not source:
            self.status_var.set("已保存显示板块筛选设置")
            return
        try:
            self.update_result_table(source, announce=False, persist=False)
            self.status_var.set(f"已按筛选条件更新，当前显示 {len(self.filtered_stocks)} 只")
        except ValueError as exc:
            messagebox.showerror("错误", str(exc))

    def on_price_filter_changed(self, event=None):
        if self.is_scanning:
            return "break" if event is not None else None
        source = self.all_scan_results or self.filtered_stocks
        if not source:
            self.status_var.set("当前没有可筛选结果")
            return "break" if event is not None else None
        try:
            self._apply_result_filters(source, raise_price_error=True)
            self.update_result_table(source, announce=False, persist=False)
            self.status_var.set(f"已按价格过滤，当前显示 {len(self.filtered_stocks)} 只")
        except ValueError as exc:
            messagebox.showerror("错误", str(exc))
        return "break" if event is not None else None

    def clear_price_filter(self):
        self.min_price_var.set("")
        self.max_price_var.set("")
        self.on_price_filter_changed()

    def on_history_source_changed(self, event=None):
        self._save_app_settings()
        source = str(self.history_source_var.get() or "auto").strip().lower() or "auto"
        self._apply_source_preferences()
        self.status_var.set(f"历史数据源已切换为 {source}")
        self._log(
            f"数据源设置已更新: history={self.history_source_var.get()}, intraday={self.intraday_source_var.get()}, fund_flow={self.fund_flow_source_var.get()}, limit_up_reason={self.limit_up_reason_source_var.get()}"
        )
        return None

    def _scan_signature(self, request: ScanRequest) -> Dict[str, Any]:
        return request.to_signature()

    def _save_last_results(
        self,
        results: List[Dict[str, Any]],
        complete: bool = True,
        request: Optional[ScanRequest] = None,
    ) -> None:
        payload_results = []
        for result in results:
            data = result.get("data", {}) or {}
            analysis = data.get("analysis") or {}
            payload_results.append(
                {
                    "code": result.get("code", ""),
                    "name": result.get("name", ""),
                    "passed": bool(result.get("passed")),
                    "reasons": result.get("reasons", []),
                    "data": {
                        "board": data.get("board", ""),
                        "exchange": data.get("exchange", ""),
                        "analysis": analysis,
                    },
                }
            )
        signature_request = request or self._active_scan_request
        if signature_request is None:
            current_settings = self.stock_filter.get_settings()
            signature_request = ScanRequest(
                filter_settings=current_settings,
                max_stocks=int(self._current_scan_max_stocks),
                allowed_boards=tuple(self._current_scan_allowed_boards),
            )
        payload = {
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_date": datetime.now().strftime("%Y-%m-%d"),
            "complete": bool(complete),
            "row_count": len(payload_results),
            "signature": self._scan_signature(signature_request),
            "results": payload_results,
        }
        save_scan_snapshot(json.dumps(payload["signature"], ensure_ascii=False, sort_keys=True), payload)

    def _load_last_results(self) -> None:
        payload = load_latest_scan_snapshot()
        if not payload:
            return
        results = payload.get("results", []) or []
        if not results:
            return
        self.all_scan_results = list(results)
        self.filtered_stocks = list(results)
        self.update_result_table(results, announce=False, persist=False)
        self.status_var.set("已从本地结果恢复")

    def _can_use_snapshot(self, request: ScanRequest) -> bool:
        if request.ignore_result_snapshot:
            return False
        if request.refresh_universe:
            return False
        signature = self._scan_signature(request)
        payload = load_scan_snapshot(json.dumps(signature, ensure_ascii=False, sort_keys=True))
        return bool(payload and payload.get("complete") and payload.get("results"))

    def update_filter_params(self) -> bool:
        return self._apply_filter_settings_from_ui(show_error=True) is not None

    def _history_cache_summary_text(self) -> str:
        summary = self.stock_filter.fetcher.get_history_cache_summary()
        return (
            f"历史缓存 {summary.get('covered_count', 0)}/{summary.get('universe_count', 0)} "
            f"({summary.get('coverage_ratio', 0.0) * 100:.1f}%)，最新交易日 {summary.get('latest_trade_date') or '-'}"
        )

    def start_scan(self):
        if self.is_scanning or self.is_updating_cache:
            return
        request = self._build_scan_request()
        if request is None:
            return

        self._active_scan_request = request
        self._current_scan_allowed_boards = list(request.allowed_boards)
        self._current_scan_max_stocks = int(request.max_stocks)

        if self._can_use_snapshot(request):
            self._log("命中本地结果快照，直接恢复上次扫描结果。")
            signature = json.dumps(self._scan_signature(request), ensure_ascii=False, sort_keys=True)
            payload = load_scan_snapshot(signature)
            if payload:
                self.all_scan_results = payload.get("results", []) or []
                self.update_result_table(self.all_scan_results, announce=False, persist=False)
                self.status_var.set("已从本地结果恢复")
                self.progress_var.set(100)
                self.scan_finished("已从本地结果恢复")
                return

        self.is_scanning = True
        self.scan_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.all_scan_results.clear()
        self.filtered_stocks.clear()
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        self._open_run_log()
        self._log(
            f"开始扫描：最近{request.filter_settings.trend_days}日收盘 > MA{request.filter_settings.ma_period}，"
            f"近{request.filter_settings.limit_up_lookback_days}日内涨停过滤={'开' if request.filter_settings.require_limit_up_within_days else '关'}。"
        )
        self._log(self._history_cache_summary_text())
        self._log(f"本轮历史数据源：{request.history_source}")
        if request.max_stocks <= 0:
            self._log("本次为全量扫描，建议优先在收盘后执行，并尽量复用本地结果快照。")
        if request.scan_workers >= 4:
            self._log(
                f"你当前设置了 {request.scan_workers} 个扫描线程；程序会自动做并发保护，但较大的线程数仍可能增加外部接口压力。"
            )
        if request.refresh_universe:
            self._log("已开启“重新拉取股票池”，本轮会刷新股票池缓存，整体耗时会更长。")
        if request.ignore_result_snapshot:
            self._log("已开启“忽略本地结果快照”，本轮不会直接复用上次扫描结果。")
        self._log("扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
        self.status_var.set("正在扫描...")
        self.progress_var.set(0)

        self._scan_thread = threading.Thread(target=self.scan_stocks, args=(request,), daemon=True)
        self._scan_thread.start()

    def start_history_cache_update(self):
        if self.is_scanning or self.is_updating_cache:
            return
        request = self._build_scan_request()
        if request is None:
            return
        self.is_updating_cache = True
        self.scan_btn.config(state=tk.DISABLED)
        self.update_cache_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self._open_run_log()
        self._log("开始更新历史缓存。")
        self._log(self._history_cache_summary_text())
        self.status_var.set("正在更新历史缓存...")
        self.progress_var.set(0)
        self._cache_thread = threading.Thread(target=self.update_history_cache, args=(request,), daemon=True)
        self._cache_thread.start()

    def stop_scan(self):
        self.is_scanning = False
        self.is_updating_cache = False
        self.status_var.set("正在停止...")
        self._log("已请求停止，正在等待当前任务结束。")

    def scan_stocks(self, request: ScanRequest):
        import time as _time
        try:
            scan_filter = StockFilter()
            scan_filter.apply_settings(request.filter_settings)
            scan_filter.set_log_callback(self._log_async)
            self._log_async(
                f"扫描参数：数量={'全量' if request.max_stocks <= 0 else request.max_stocks}，并发线程={request.scan_workers}，历史源={request.history_source}"
            )

            scan_t0 = _time.time()

            def progress_callback(current, total, code, name):
                if not self.is_scanning:
                    raise StopIteration
                progress = (current / total) * 100 if total else 0
                elapsed = _time.time() - scan_t0
                speed = current / elapsed if elapsed > 0 else 0
                eta_sec = (total - current) / speed if speed > 0 else 0
                if eta_sec >= 60:
                    eta_text = f"{int(eta_sec // 60)}分{int(eta_sec % 60)}秒"
                else:
                    eta_text = f"{int(eta_sec)}秒"
                status_text = f"扫描中 {current}/{total} ({progress:.0f}%) | {speed:.1f}只/秒 | 剩余 {eta_text}"
                self.root.after(0, lambda: self.progress_var.set(progress))
                self.root.after(0, lambda s=status_text: self.status_var.set(s))

            results = scan_filter.scan_all_stocks(
                max_stocks=request.max_stocks,
                progress_callback=progress_callback,
                max_workers=request.scan_workers,
                history_source=request.history_source,
                local_history_only=True,
                should_stop=lambda: not self.is_scanning,
                refresh_universe=request.refresh_universe,
                allowed_boards=list(request.allowed_boards),
            )
            if not self.is_scanning:
                self.root.after(0, lambda: self._log("扫描已停止。"))
                self.root.after(0, lambda: self.scan_finished("扫描已停止"))
                return
            self.all_scan_results = results
            self.root.after(0, lambda res=results, req=request: self.update_result_table(res, request=req))
            self.root.after(0, lambda count=len(results): self.scan_finished(f"扫描完成，命中 {count} 只。"))
        except StopIteration:
            self.root.after(0, lambda: self._log("扫描已停止。"))
            self.root.after(0, lambda: self.scan_finished("扫描已停止"))
        except Exception as e:
            error_text = str(e)
            self.root.after(0, lambda: self._log(f"扫描出错: {error_text}"))
            self.root.after(0, lambda: self.scan_finished(f"扫描失败: {error_text}"))

    def update_history_cache(self, request: ScanRequest):
        import time as _time
        try:
            scan_filter = StockFilter()
            scan_filter.apply_settings(request.filter_settings)
            scan_filter.set_log_callback(self._log_async)
            scan_filter.set_history_source_preference(request.history_source)
            self._log_async(
                f"缓存更新参数：数量={'全量' if request.max_stocks <= 0 else request.max_stocks}，并发线程={request.scan_workers}，历史源={request.history_source}"
            )

            cache_t0 = _time.time()
            cache_updated = 0
            cache_failed = 0
            cache_skipped = 0

            def progress_callback(current, total, code, name):
                if not self.is_updating_cache:
                    raise StopIteration
                progress = (current / total) * 100 if total else 0
                elapsed = _time.time() - cache_t0
                speed = current / elapsed if elapsed > 0 else 0
                eta_sec = (total - current) / speed if speed > 0 else 0
                if eta_sec >= 60:
                    eta_text = f"{int(eta_sec // 60)}分{int(eta_sec % 60)}秒"
                else:
                    eta_text = f"{int(eta_sec)}秒"
                status_text = (
                    f"更新缓存 {current}/{total} ({progress:.0f}%) "
                    f"| 速度 {speed:.1f}只/秒 | 预计剩余 {eta_text} "
                    f"| 跳过{cache_skipped} 失败{cache_failed}"
                )
                self.root.after(0, lambda: self.progress_var.set(progress))
                self.root.after(0, lambda s=status_text: self.status_var.set(s))

            result = scan_filter.fetcher.update_history_cache(
                max_stocks=request.max_stocks,
                days=max(60, request.filter_settings.ma_period + request.filter_settings.limit_up_lookback_days + 20),
                source=request.history_source,
                workers=request.scan_workers,
                progress_callback=progress_callback,
                should_stop=lambda: not self.is_updating_cache,
                refresh_universe=request.refresh_universe,
                allowed_boards=list(request.allowed_boards),
            )
            cache_updated = result.get("updated", 0)
            cache_failed = result.get("failed", 0)
            cache_skipped = result.get("skipped", 0)
            if not self.is_updating_cache:
                self.root.after(0, lambda: self._log("历史缓存更新已停止。"))
                self.root.after(0, lambda: self.scan_finished("历史缓存更新已停止"))
                return
            total_time = _time.time() - cache_t0
            if total_time >= 60:
                time_text = f"{int(total_time // 60)}分{int(total_time % 60)}秒"
            else:
                time_text = f"{total_time:.1f}秒"
            summary_msg = (
                f"历史缓存更新完成：总计 {result.get('total', 0)}，"
                f"成功 {cache_updated}，跳过(已新鲜) {cache_skipped}，失败 {cache_failed}，"
                f"耗时 {time_text}。"
            )
            self.root.after(0, lambda m=summary_msg: self._log(m))
            self.root.after(0, lambda: self._log(self._history_cache_summary_text()))
            self.root.after(0, lambda: self.scan_finished("历史缓存更新完成"))
        except StopIteration:
            self.root.after(0, lambda: self._log("历史缓存更新已停止。"))
            self.root.after(0, lambda: self.scan_finished("历史缓存更新已停止"))
        except Exception as e:
            error_text = str(e)
            self.root.after(0, lambda: self._log(f"历史缓存更新出错: {error_text}"))
            self.root.after(0, lambda: self.scan_finished(f"历史缓存更新失败: {error_text}"))

    def _sort_value_for_column(self, item: Dict[str, Any], column: str):
        data = item.get("data", {}) or {}
        analysis = data.get("analysis") or {}

        if column == "code":
            return str(item.get("code", "")).zfill(6)
        if column == "name":
            return str(item.get("name", ""))
        if column == "board":
            return str(data.get("board") or data.get("exchange") or "")
        if column == "latest_close":
            value = analysis.get("latest_close")
            return float(value) if value is not None else float("-inf")
        if column == "latest_ma":
            value = analysis.get("latest_ma")
            return float(value) if value is not None else float("-inf")
        if column == "five_day_return":
            value = analysis.get("five_day_return")
            return float(value) if value is not None else float("-inf")
        if column == "limit_up_streak":
            return int(analysis.get("limit_up_streak") or 0)
        if column == "broken_limit_up":
            return 1 if analysis.get("broken_limit_up") else 0
        if column == "volume_expand_ratio":
            value = analysis.get("volume_expand_ratio")
            return float(value) if value is not None else float("-inf")
        if column == "volume_expand":
            return 1 if analysis.get("volume_expand") else 0
        if column == "volume_break_limit_up":
            return 1 if analysis.get("volume_break_limit_up") else 0
        if column == "after_two_limit_up":
            return 1 if analysis.get("after_two_limit_up") else 0
        if column == "limit_up":
            return 1 if analysis.get("limit_up") else 0
        if column == "recent_closes":
            recent = analysis.get("recent_closes") or []
            return tuple("" if v is None else f"{float(v):010.4f}" for v in recent)
        latest_change_pct = analysis.get("latest_change_pct")
        return float(latest_change_pct) if latest_change_pct is not None else float("-inf")

    def _sort_results(
        self,
        results: List[Dict[str, Any]],
        column: Optional[str] = None,
        reverse: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        sort_column = column or self.sort_column
        sort_reverse = self.sort_reverse if reverse is None else bool(reverse)
        secondary_columns = [
            "limit_up_streak",
            "volume_break_limit_up",
            "volume_expand_ratio",
            "limit_up",
            "five_day_return",
            "latest_change_pct",
        ]
        if sort_column in secondary_columns:
            secondary_columns = [c for c in secondary_columns if c != sort_column]
        return sorted(
            results,
            key=lambda item: tuple(
                [self._sort_value_for_column(item, sort_column)]
                + [self._sort_value_for_column(item, c) for c in secondary_columns]
                + [str(item.get("code", ""))]
            ),
            reverse=sort_reverse,
        )

    def on_result_heading_click(self, column: str):
        if column == self.sort_column:
            self.sort_reverse = not self.sort_reverse
        else:
            self.sort_column = column
            self.sort_reverse = column in {
                "five_day_return",
                "limit_up_streak",
                "broken_limit_up",
                "latest_close",
                "latest_ma",
                "volume_expand_ratio",
                "volume_break_limit_up",
                "after_two_limit_up",
                "limit_up",
                "volume_expand",
            }
        if self.filtered_stocks:
            self.update_result_table(self.filtered_stocks, announce=False, persist=False)

    def update_result_table(
        self,
        results: List[Dict[str, Any]],
        announce: bool = True,
        persist: bool = True,
        request: Optional[ScanRequest] = None,
    ):
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        results = self._apply_result_filters(results)
        results = self._sort_results(results)

        for result in results:
            row_values = self._format_result_row_values(result)
            values = tuple(row_values.get(col, "-") for col in self.result_columns)
            self.result_tree.insert("", tk.END, values=values)

        self.filtered_stocks = results
        if persist:
            self._save_last_results(results, complete=True, request=request)
        if announce:
            self._log(f"扫描完成，命中 {len(results)} 只。")

    def scan_finished(self, status_text: str = "扫描完成"):
        self.scan_btn.config(state=tk.NORMAL)
        self.update_cache_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_var.set(status_text)
        self.is_scanning = False
        self.is_updating_cache = False
        self._scan_thread = None
        self._cache_thread = None
        self._active_scan_request = None
        self.refresh_universe_var.set(False)
        self._close_run_log()

    def on_stock_select(self, event):
        try:
            selection = self.result_tree.selection()
            if not selection:
                return
            item = self.result_tree.item(selection[0])
            values = item.get("values") or []
            if not values:
                return
            self._schedule_show_stock_detail(values[0])
        except Exception as e:
            self._log(f"选择股票详情失败: {e}")

    def on_stock_double_click(self, event):
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item["values"][0]
            self._cancel_scheduled_detail()
            self.show_stock_detail(stock_code, force_refresh=True)
            self.notebook.select(1)

    def query_single_stock(self):
        stock_code = self.stock_code_var.get().strip()
        if not stock_code:
            messagebox.showwarning("警告", "请输入股票代码")
            return
        if not self.update_filter_params():
            return
        self._cancel_scheduled_detail()
        self.show_stock_detail(stock_code, force_refresh=True)
        self.notebook.select(1)

    def _cancel_scheduled_detail(self) -> None:
        if self._detail_after_id is None:
            return
        try:
            self.root.after_cancel(self._detail_after_id)
        except tk.TclError:
            pass
        self._detail_after_id = None

    def _schedule_show_stock_detail(self, stock_code: str, delay_ms: int = 180) -> None:
        code = str(stock_code).strip().zfill(6)
        self._cancel_scheduled_detail()
        self._detail_after_id = self.root.after(delay_ms, lambda c=code: self.show_stock_detail(c))

    def show_stock_detail(self, stock_code: str, force_refresh: bool = False):
        code = str(stock_code).strip().zfill(6)
        self._cancel_scheduled_detail()
        self._detail_request_code = code
        self._show_detail_loading(code)

        detail_payload = None
        for result in self.filtered_stocks:
            if str(result.get("code", "")).strip().zfill(6) == code:
                data = result.get("data", {}) or {}
                detail_payload = {
                    "code": code,
                    "name": result.get("name", ""),
                    "board": data.get("board", ""),
                    "exchange": data.get("exchange", ""),
                    "history": data.get("history"),
                    "analysis": data.get("analysis") or {},
                }
                break

        if detail_payload is not None:
            self._log(f"显示扫描结果中的股票 {code} 详情。")
            try:
                self._update_detail_ui(detail_payload)
            except Exception as e:
                self._log(f"渲染缓存详情失败: {e}")
                self._show_detail_error(code, f"渲染详情失败: {e}")
            if not force_refresh:
                # 扫描结果中已有完整数据，直接展示不再发起后台刷新。
                # get_history_data 内部的缓存新鲜度机制会确保数据时效性。
                self.status_var.set(f"{code} 详情（缓存）")
                self._detail_loading_code = ""
                return
            self.status_var.set(f"正在刷新 {code} 最新详情...")
            self._detail_loading_code = code
            detail_thread = threading.Thread(target=self._load_detail, args=(code,), daemon=True)
            detail_thread.start()
            return

        if not force_refresh and self._detail_loading_code == code:
            self.status_var.set(f"{code} 详情正在加载...")
            return

        self._log(f"查询股票 {code} 的历史详情...")
        self.status_var.set(f"正在查询 {code}...")
        self._detail_loading_code = code
        detail_thread = threading.Thread(target=self._load_detail, args=(code,), daemon=True)
        detail_thread.start()
    def _load_detail(self, stock_code: str):
        try:
            detail = self.stock_filter.get_stock_detail(stock_code)
            self.root.after(0, lambda: self._apply_detail_if_current(stock_code, detail))
        except Exception as e:
            error_text = str(e)
            self.root.after(0, lambda: self._log(f"查询详情出错: {error_text}"))
            self.root.after(0, lambda: self._show_detail_error(stock_code, f"详情加载失败: {error_text}"))
        finally:
            self.root.after(0, lambda: self._finish_detail_status(stock_code))

    def _apply_detail_if_current(self, stock_code: str, detail: Dict[str, Any]) -> None:
        if str(stock_code).strip().zfill(6) != self._detail_request_code:
            return
        try:
            self._update_detail_ui(detail)
        except Exception as e:
            self._log(f"更新详情面板失败: {e}")
            self._show_detail_error(stock_code, f"详情渲染失败: {e}")
    def _finish_detail_status(self, stock_code: str) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self._detail_loading_code:
            self._detail_loading_code = ""
        if code != self._detail_request_code:
            return
        self.status_var.set("查询完成")

    def _show_detail_loading(self, stock_code: str) -> None:
        self._detail_chart_dates = []
        self._detail_chart_history = None
        self._detail_chart_analysis = {}
        self._detail_chart_window_start = 0
        self._detail_chart_loaded_days = 0
        self._detail_chart_loading_more = False
        placeholders = {
            "code": str(stock_code).strip().zfill(6),
            "name": "加载中...",
            "latest_date": "加载中...",
            "quote_time": "加载中...",
            "latest_close": "加载中...",
            "latest_ma": "加载中...",
            "latest_ma10": "加载中...",
            "latest_volume": "加载中...",
            "latest_amount": "加载中...",
            "five_day_return": "加载中...",
            "limit_up": "加载中...",
            "big_order_amount": "加载中...",
            "main_force_amount": "加载中...",
            "summary": "正在加载详情数据...",
        }
        for key, value in placeholders.items():
            if key in self.detail_labels:
                self.detail_labels[key].config(text=value)
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.text(0.5, 0.5, "正在加载详情...", ha="center", va="center", fontsize=14)
        self.volume_ax.text(0.5, 0.5, "请稍候", ha="center", va="center", fontsize=11)
        self.flow_ax.text(0.5, 0.5, "正在加载大单净额...", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.flow_ax.set_axis_off()
        self.canvas.draw()

    def _show_detail_error(self, stock_code: str, message: str) -> None:
        self._detail_chart_dates = []
        self._detail_chart_history = None
        self._detail_chart_analysis = {}
        self._detail_chart_window_start = 0
        self._detail_chart_loaded_days = 0
        self._detail_chart_loading_more = False
        code_text = str(stock_code).strip().zfill(6)
        if "code" in self.detail_labels:
            self.detail_labels["code"].config(text=code_text)
        if "name" in self.detail_labels:
            self.detail_labels["name"].config(text="加载失败")
        if "summary" in self.detail_labels:
            self.detail_labels["summary"].config(text=message or "详情加载失败")
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.text(0.5, 0.5, "详情加载失败", ha="center", va="center", fontsize=14, color="#b22222")
        self.volume_ax.text(0.5, 0.5, "请查看运行日志", ha="center", va="center", fontsize=11)
        self.flow_ax.text(0.5, 0.5, "请稍后重试", ha="center", va="center", fontsize=11)
        self.price_ax.set_axis_off()
        self.volume_ax.set_axis_off()
        self.flow_ax.set_axis_off()
        self.canvas.draw()
    def _update_detail_ui(self, detail: Dict[str, Any]):
        analysis = detail.get("analysis") or {}
        history = detail.get("history")
        self._current_detail_code = str(detail.get("code", "") or "").strip().zfill(6)
        self._detail_chart_loading_more = False
        self._refresh_detail_metric_labels()

        self.detail_labels["code"].config(text=detail.get("code", "-"))
        self.detail_labels["name"].config(text=detail.get("name", "-"))
        self.detail_labels["latest_date"].config(text=analysis.get("latest_date", "-"))
        self.detail_labels["quote_time"].config(text=analysis.get("quote_time", "-") or "-")
        self.detail_labels["latest_close"].config(
            text="-" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}"
        )
        self.detail_labels["latest_ma"].config(
            text="-" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}"
        )
        self.detail_labels["latest_ma10"].config(
            text="-" if analysis.get("latest_ma10") is None else f"{analysis['latest_ma10']:.2f}"
        )
        latest_volume_text = self._format_volume(analysis.get("latest_volume"))
        latest_volume_ratio = analysis.get("latest_volume_ratio")
        if latest_volume_text != "-" and latest_volume_ratio is not None:
            latest_volume_text = f"{latest_volume_text} ({latest_volume_ratio:.1f}%)"
        self.detail_labels["latest_volume"].config(text=latest_volume_text)
        self.detail_labels["latest_amount"].config(text=self._format_amount(analysis.get("latest_amount")))
        self.detail_labels["five_day_return"].config(
            text="-" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%"
        )
        self.detail_labels["limit_up"].config(text="是" if analysis.get("limit_up") else "否")
        self.detail_labels["big_order_amount"].config(text=self._format_amount(analysis.get("big_order_amount")))
        self.detail_labels["main_force_amount"].config(text=self._format_amount(analysis.get("main_force_amount")))
        self.detail_labels["summary"].config(text=analysis.get("summary", "-"))

        self._draw_chart(history, analysis)

    def _reset_detail_chart_axes(self) -> None:
        self._detail_chart_dates = []
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.set_axis_on()
        self.volume_ax.set_axis_on()
        self.flow_ax.set_axis_on()

    def _show_empty_detail_chart(self, message: str) -> None:
        self._detail_chart_history = None
        self._detail_chart_analysis = {}
        self._detail_chart_window_start = 0
        self.detail_chart_window_scale.config(from_=0, to=0, state=tk.DISABLED)
        self.detail_chart_window_var.set(0)
        self.detail_chart_window_label_var.set("窗口: -")
        self.price_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=14)
        self.canvas.draw()

    def _prepare_detail_chart_dataset(self, history, analysis):
        if history is None or getattr(history, "empty", True):
            return None

        df = history.copy()
        if "date" in df.columns:
            df = df.sort_values("date").reset_index(drop=True)
        else:
            df = df.reset_index(drop=True)
        if df.empty:
            return None

        self._detail_chart_history = df
        self._detail_chart_analysis = dict(analysis or {})
        self._detail_chart_loaded_days = max(self._detail_chart_loaded_days, len(df))

        x = list(range(len(df)))
        dates = df["date"].astype(str).tolist() if "date" in df.columns else [str(i) for i in x]
        self._detail_chart_dates = list(dates)
        opens = pd.to_numeric(df["open"], errors="coerce") if "open" in df.columns else pd.Series([None] * len(df))
        closes = pd.to_numeric(df["close"], errors="coerce") if "close" in df.columns else pd.Series([None] * len(df))
        highs = pd.to_numeric(df["high"], errors="coerce") if "high" in df.columns else pd.Series([None] * len(df))
        lows = pd.to_numeric(df["low"], errors="coerce") if "low" in df.columns else pd.Series([None] * len(df))
        volumes = pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else pd.Series([0] * len(df))
        return {
            "df": df,
            "x": x,
            "dates": dates,
            "opens": opens,
            "closes": closes,
            "highs": highs,
            "lows": lows,
            "volumes": volumes,
        }

    def _draw_detail_price_panel(self, chart_data: Dict[str, Any]) -> None:
        x = chart_data["x"]
        opens = chart_data["opens"]
        closes = chart_data["closes"]
        highs = chart_data["highs"]
        lows = chart_data["lows"]

        for idx, (open_price, close_price, high_price, low_price) in enumerate(zip(opens, closes, highs, lows)):
            if pd.isna(open_price) or pd.isna(close_price) or pd.isna(high_price) or pd.isna(low_price):
                continue
            color = "#d94b4b" if close_price >= open_price else "#1f8b4c"
            body_low = min(open_price, close_price)
            body_height = abs(close_price - open_price)
            if body_height == 0:
                body_height = max(close_price * 0.001, 0.01)
            self.price_ax.vlines(idx, low_price, high_price, color=color, linewidth=1.0)
            self.price_ax.bar(idx, body_height, bottom=body_low, width=0.6, color=color, edgecolor=color, alpha=0.85)

        ma_period = max(1, int(self.stock_filter.ma_period))
        ma = closes.rolling(window=ma_period, min_periods=ma_period).mean()
        ma10 = closes.rolling(window=10, min_periods=10).mean()
        latest_close = closes.dropna().iloc[-1] if not closes.dropna().empty else None
        latest_ma = ma.dropna().iloc[-1] if not ma.dropna().empty else None
        latest_ma10 = ma10.dropna().iloc[-1] if not ma10.dropna().empty else None
        close_label = "收盘价" if latest_close is None else f"收盘价 {latest_close:.2f}"
        ma_label = f"MA{ma_period}" if latest_ma is None else f"MA{ma_period} {latest_ma:.2f}"
        ma10_label = "MA10" if latest_ma10 is None else f"MA10 {latest_ma10:.2f}"
        self.price_ax.plot(x, closes, color="#2f6fd6", linewidth=1.0, alpha=0.35, label=close_label)
        self.price_ax.plot(x, ma, color="#f08a24", linewidth=1.4, label=ma_label)
        self.price_ax.plot(x, ma10, color="#7b52ab", linewidth=1.2, label=ma10_label)
        self.price_ax.set_ylabel("价格")
        self.price_ax.set_title("K线图（滚轮左右滑动，点击K线进入分时）")
        self.price_ax.legend(loc="upper left")
        self.price_ax.grid(True, alpha=0.25)

    def _build_detail_flow_series(self, dates: List[str], analysis: Dict[str, Any]):
        flow_history = (analysis or {}).get("fund_flow_history") or []
        flow_map = {}
        for item in flow_history:
            if not isinstance(item, dict):
                continue
            flow_date = str(item.get("date", "") or "").strip()
            if flow_date:
                flow_map[flow_date] = item

        values = []
        colors = []
        for date_str in dates:
            flow_item = flow_map.get(date_str, {})
            amount = pd.to_numeric(pd.Series([flow_item.get("big_order_amount")]), errors="coerce").iloc[0]
            if pd.isna(amount):
                amount = 0.0
            values.append(float(amount))
            colors.append("#d94b4b" if amount >= 0 else "#1f8b4c")
        return values, colors

    def _draw_detail_volume_panel(self, chart_data: Dict[str, Any]) -> None:
        x = chart_data["x"]
        opens = chart_data["opens"]
        closes = chart_data["closes"]
        volumes = chart_data["volumes"]
        volume_colors = [
            "#d94b4b" if (not pd.isna(c) and not pd.isna(o) and c >= o) else "#1f8b4c"
            for o, c in zip(opens, closes)
        ]
        self.volume_ax.bar(x, volumes.fillna(0), width=0.6, color=volume_colors, alpha=0.85)
        volume_compare_window = volumes.iloc[-6:-1].dropna() if len(volumes) > 1 else pd.Series(dtype=float)
        if volume_compare_window.empty:
            volume_compare_window = volumes.dropna()
        latest_volume_value = volumes.dropna().iloc[-1] if not volumes.dropna().empty else None
        latest_volume_ratio_text = ""
        if latest_volume_value is not None and not volume_compare_window.empty:
            avg_volume = float(volume_compare_window.mean())
            if avg_volume > 0:
                latest_volume_ratio_text = f"  最新 {self._format_volume(latest_volume_value)} / 均量 {latest_volume_value / avg_volume * 100.0:.1f}%"
        self.volume_ax.set_ylabel("成交量")
        self.volume_ax.set_title(f"成交量{latest_volume_ratio_text}" if latest_volume_ratio_text else "成交量")
        self.volume_ax.yaxis.set_major_formatter(FuncFormatter(self._format_axis_volume))
        self.volume_ax.yaxis.get_offset_text().set_visible(False)
        self.volume_ax.grid(True, alpha=0.2)

    def _draw_detail_flow_panel(self, chart_data: Dict[str, Any], analysis: Dict[str, Any]) -> None:
        if not self._detail_flow_expanded:
            self.flow_ax.set_visible(False)
            self.flow_ax.set_axis_off()
            return

        flow_values, flow_colors = self._build_detail_flow_series(chart_data["dates"], analysis)
        self.flow_ax.set_visible(True)
        self.flow_ax.bar(chart_data["x"], flow_values, width=0.6, color=flow_colors, alpha=0.85)
        self.flow_ax.axhline(0, color="#666666", linewidth=0.8, alpha=0.6)
        self.flow_ax.set_ylabel("大单净额")
        self.flow_ax.set_xlabel("日期")
        self.flow_ax.grid(True, alpha=0.2)

    def _resolve_detail_chart_window(self, total: int, keep_window: bool = False) -> Dict[str, int]:
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        start = max(0, min(int(self._detail_chart_window_start), max_start)) if keep_window else max_start
        end = min(total, start + window)
        return {"window": window, "max_start": max_start, "start": start, "end": end}

    def _apply_detail_chart_window(self, chart_data: Dict[str, Any], window_meta: Dict[str, int]) -> None:
        x = chart_data["x"]
        dates = chart_data["dates"]
        start = window_meta["start"]
        end = window_meta["end"]
        max_start = window_meta["max_start"]

        self._detail_chart_window_start = start
        self.detail_chart_window_scale.config(from_=0, to=max_start, state=(tk.NORMAL if max_start > 0 else tk.DISABLED))
        self._detail_chart_slider_updating = True
        try:
            self.detail_chart_window_var.set(start)
        finally:
            self._detail_chart_slider_updating = False

        if x:
            self.price_ax.set_xlim(start - 0.5, end - 0.5)
            self.volume_ax.set_xlim(start - 0.5, end - 0.5)
            if self._detail_flow_expanded:
                self.flow_ax.set_xlim(start - 0.5, end - 0.5)

        view_len = max(1, end - start)
        tick_step = max(1, view_len // 8)
        tick_positions = list(range(start, end, tick_step))
        if tick_positions and tick_positions[-1] != end - 1:
            tick_positions.append(end - 1)
        elif not tick_positions and end > start:
            tick_positions = [end - 1]
        tick_labels = [dates[pos][5:] if len(dates[pos]) >= 10 else dates[pos] for pos in tick_positions]

        self.price_ax.set_xticks(tick_positions)
        self.price_ax.tick_params(axis="x", labelbottom=False)
        if self._detail_flow_expanded:
            self.volume_ax.set_xticklabels([""] * len(tick_positions))
            self.volume_ax.tick_params(axis="x", labelbottom=False)
            self.flow_ax.set_xticklabels(tick_labels, rotation=45, ha="right")
            self.flow_ax.tick_params(axis="x", labelbottom=True)
        else:
            self.flow_ax.tick_params(axis="x", labelbottom=False)
            self.volume_ax.set_xticklabels(tick_labels, rotation=45, ha="right")
            self.volume_ax.tick_params(axis="x", labelbottom=True)

        if dates:
            self.detail_chart_window_label_var.set(f"窗口: {dates[start]} ~ {dates[end - 1]}")
        else:
            self.detail_chart_window_label_var.set("窗口: -")

    def _draw_chart(self, history, analysis, keep_window: bool = False):
        self._reset_detail_chart_axes()

        chart_data = self._prepare_detail_chart_dataset(history, analysis)
        if chart_data is None:
            self._show_empty_detail_chart("暂无历史数据")
            return

        self._draw_detail_price_panel(chart_data)
        self._draw_detail_volume_panel(chart_data)
        self._draw_detail_flow_panel(chart_data, analysis or {})
        window_meta = self._resolve_detail_chart_window(len(chart_data["x"]), keep_window=keep_window)
        self._apply_detail_chart_window(chart_data, window_meta)
        self.fig.tight_layout()
        self.canvas.draw()

    def toggle_detail_summary_section(self):
        self._detail_summary_expanded = not self._detail_summary_expanded
        self._apply_detail_section_visibility()

    def toggle_detail_chart_section(self):
        self._detail_chart_expanded = not self._detail_chart_expanded
        self._apply_detail_section_visibility()

    def _apply_detail_section_visibility(self):
        if self._detail_summary_expanded:
            if not self.info_frame.winfo_ismapped():
                self.info_frame.pack(fill=tk.X, pady=5, after=self.detail_info_header)
            self.detail_summary_toggle_btn.config(text="收起历史摘要")
            self.detail_summary_status_var.set("历史摘要已展开")
        else:
            if self.info_frame.winfo_ismapped():
                self.info_frame.pack_forget()
            self.detail_summary_toggle_btn.config(text="展开历史摘要")
            self.detail_summary_status_var.set("历史摘要已收起")

        if self._detail_chart_expanded:
            if not self.chart_frame.winfo_ismapped():
                self.chart_frame.pack(fill=tk.BOTH, expand=True, pady=5, after=self.detail_chart_header)
            self.detail_chart_toggle_btn.config(text="收起历史K线")
            self.detail_chart_status_var.set("历史K线已展开")
        else:
            if self.chart_frame.winfo_ismapped():
                self.chart_frame.pack_forget()
            self.detail_chart_toggle_btn.config(text="展开历史K线")
            self.detail_chart_status_var.set("历史K线已收起")

    def toggle_detail_flow_section(self):
        self._detail_flow_expanded = not self._detail_flow_expanded
        if self._detail_flow_expanded:
            self.detail_flow_toggle_btn.config(text="收起大单净额")
            self.detail_flow_status_var.set("大单净额已展开")
        else:
            self.detail_flow_toggle_btn.config(text="展开大单净额")
            self.detail_flow_status_var.set("大单净额已收起")
        if self._detail_chart_history is not None:
            self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)

    def on_detail_chart_window_changed(self, value):
        if self._detail_chart_slider_updating:
            return
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return
        try:
            new_start = int(float(value))
        except (TypeError, ValueError):
            return
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        new_start = max(0, min(new_start, max_start))
        if new_start == self._detail_chart_window_start:
            return
        self._detail_chart_window_start = new_start
        self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)

    def _bind_detail_chart_scroll(self, widget) -> None:
        if self._detail_chart_scroll_bound:
            return
        try:
            widget.bind("<MouseWheel>", self.on_detail_chart_mousewheel)
            widget.bind("<Shift-MouseWheel>", self.on_detail_chart_mousewheel)
            widget.bind("<Button-4>", self.on_detail_chart_mousewheel)
            widget.bind("<Button-5>", self.on_detail_chart_mousewheel)
            widget.bind("<Left>", lambda _e: self._move_detail_chart_window(-12))
            widget.bind("<Right>", lambda _e: self._move_detail_chart_window(12))
            widget.bind("<Enter>", lambda _e: widget.focus_set())
            widget.bind("<Button-1>", lambda _e: widget.focus_set())
            self._detail_chart_scroll_bound = True
        except tk.TclError:
            pass

    def _move_detail_chart_window(self, delta: int) -> bool:
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return False
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            self._maybe_load_more_detail_history(need_older=True)
            return False
        try:
            shift = int(delta)
        except (TypeError, ValueError):
            return False
        if shift == 0:
            return False
        new_start = max(0, min(self._detail_chart_window_start + shift, max_start))
        if new_start == self._detail_chart_window_start:
            if new_start == 0 and shift < 0:
                self._maybe_load_more_detail_history(need_older=True)
            return False
        self._detail_chart_window_start = new_start
        self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)
        if self._detail_chart_window_start <= max(0, min(12, max_start)) and shift < 0:
            self._maybe_load_more_detail_history(need_older=True)
        return True

    def _maybe_load_more_detail_history(self, need_older: bool = False) -> None:
        if not need_older:
            return
        if self._detail_chart_loading_more:
            return
        code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not code:
            return
        current_rows = 0
        if self._detail_chart_history is not None and not getattr(self._detail_chart_history, "empty", True):
            current_rows = len(self._detail_chart_history)
        request_days = max(120, self._detail_chart_loaded_days + 120, current_rows + 120)
        self._detail_chart_loading_more = True
        self.detail_chart_status_var.set("历史K线补载中...")
        threading.Thread(
            target=self._load_more_detail_history,
            args=(code, request_days, current_rows),
            daemon=True,
        ).start()

    def _load_more_detail_history(self, stock_code: str, request_days: int, previous_rows: int) -> None:
        history = self.stock_filter.get_stock_detail_history(stock_code, request_days)
        self.root.after(
            0,
            lambda: self._apply_more_detail_history_if_current(stock_code, history, request_days, previous_rows),
        )

    def _apply_more_detail_history_if_current(self, stock_code: str, history, request_days: int, previous_rows: int) -> None:
        self._detail_chart_loading_more = False
        code = str(stock_code).strip().zfill(6)
        if code != str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6):
            return
        if history is None or getattr(history, "empty", True):
            self.detail_chart_status_var.set("历史K线补载失败")
            return
        if len(history) <= previous_rows:
            self._detail_chart_loaded_days = max(self._detail_chart_loaded_days, request_days)
            self.detail_chart_status_var.set("历史K线已展开")
            return
        old_start = int(self._detail_chart_window_start)
        added_rows = len(history) - previous_rows
        self._detail_chart_history = history
        self._detail_chart_window_start = old_start + added_rows
        self.detail_chart_status_var.set("历史K线已展开")
        self._draw_chart(history, self._detail_chart_analysis, keep_window=True)

    def _detail_scroll_delta(self, event) -> int:
        button = str(getattr(event, "button", "") or "").lower()
        if button == "up":
            return -6
        if button == "down":
            return 6
        num = getattr(event, "num", None)
        if num == 4:
            return -6
        if num == 5:
            return 6
        delta = getattr(event, "delta", 0) or 0
        if delta > 0:
            return -6
        if delta < 0:
            return 6
        return 0

    def on_detail_chart_scroll(self, event):
        if event is None:
            return
        inaxes = getattr(event, "inaxes", None)
        if inaxes is not None and inaxes not in (self.price_ax, self.volume_ax, self.flow_ax):
            return
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            return

        delta = self._detail_scroll_delta(event)
        if delta == 0:
            return
        self._move_detail_chart_window(delta)

    def on_detail_chart_mousewheel(self, event):
        self.on_detail_chart_scroll(event)

    def on_detail_chart_click(self, event):
        if event is None:
            return
        if event.inaxes not in (self.price_ax, self.volume_ax, self.flow_ax):
            return
        if getattr(event, "button", None) not in (1, None):
            return
        self._detail_chart_click_target_date = ""
        if self._detail_chart_dates and event.xdata is not None:
            try:
                idx = int(round(float(event.xdata)))
                idx = max(0, min(idx, len(self._detail_chart_dates) - 1))
                self._detail_chart_click_target_date = str(self._detail_chart_dates[idx] or "").strip()
            except (TypeError, ValueError):
                self._detail_chart_click_target_date = ""
        if event.x is None:
            self._detail_chart_dragging = False
            return
        self._detail_chart_dragging = True
        self._detail_chart_drag_moved = False
        self._detail_chart_drag_start_x = float(event.x)
        self._detail_chart_drag_start_window = int(self._detail_chart_window_start)

    def on_detail_chart_drag_motion(self, event):
        if not self._detail_chart_dragging:
            return
        if event is None or getattr(event, "inaxes", None) not in (self.price_ax, self.volume_ax, self.flow_ax):
            return
        if self._detail_chart_history is None or getattr(self._detail_chart_history, "empty", True):
            return
        if event.x is None:
            return
        total = len(self._detail_chart_history)
        window = max(15, min(int(self._detail_chart_window_size), max(15, total)))
        max_start = max(0, total - window)
        if max_start <= 0:
            return
        pixel_per_bar = max(3.0, self.canvas.get_tk_widget().winfo_width() / max(1, window))
        bars = int(round((float(event.x) - self._detail_chart_drag_start_x) / pixel_per_bar))
        new_start = max(0, min(self._detail_chart_drag_start_window - bars, max_start))
        if new_start == self._detail_chart_window_start:
            return
        self._detail_chart_drag_moved = True
        self._detail_chart_window_start = new_start
        self._draw_chart(self._detail_chart_history, self._detail_chart_analysis, keep_window=True)

    def on_detail_chart_drag_release(self, event):
        if not self._detail_chart_dragging:
            return
        self._detail_chart_dragging = False
        if self._detail_chart_drag_moved:
            self._detail_chart_drag_moved = False
            return
        code = str(self._current_detail_code or self._detail_request_code or "").strip().zfill(6)
        if not code:
            return
        self.open_intraday_view_with_offset(code, day_offset=0, target_trade_date=self._detail_chart_click_target_date)
        self._detail_chart_drag_moved = False

    def open_intraday_view(self, stock_code: str):
        self.open_intraday_view_with_offset(stock_code, day_offset=0)

    def navigate_intraday_day(self, delta: int):
        code = str(self._intraday_request_code or self._current_detail_code or "").strip().zfill(6)
        if not code:
            return
        try:
            step = int(delta)
        except (TypeError, ValueError):
            return
        if step == 0:
            return
        if not self._intraday_available_dates:
            self.open_intraday_view_with_offset(code, day_offset=self._intraday_day_offset + step)
            return
        try:
            current_idx = self._intraday_available_dates.index(self._intraday_selected_date)
        except ValueError:
            current_idx = len(self._intraday_available_dates) - 1
        target_idx = max(0, min(current_idx + step, len(self._intraday_available_dates) - 1))
        target_date = self._intraday_available_dates[target_idx]
        self.open_intraday_view_with_offset(code, day_offset=0, target_trade_date=target_date)

    def _refresh_intraday_nav_buttons(self):
        has_code = bool(str(self._intraday_request_code or "").strip())
        has_dates = len(self._intraday_available_dates) > 0
        can_prev = has_code and has_dates and (self._intraday_day_offset > -(len(self._intraday_available_dates) - 1))
        can_next = has_code and has_dates and (self._intraday_day_offset < 0)
        self.intraday_prev_btn.config(state=(tk.NORMAL if can_prev else tk.DISABLED))
        self.intraday_next_btn.config(state=(tk.NORMAL if can_next else tk.DISABLED))

    def open_intraday_view_with_offset(self, stock_code: str, day_offset: int = 0, target_trade_date: str = ""):
        code = str(stock_code or "").strip().zfill(6)
        if not code:
            return
        last_code = str(self._intraday_request_code or "").strip().zfill(6)
        if last_code and last_code != code:
            self._intraday_available_dates = []
            self._intraday_selected_date = ""

        try:
            requested_offset = int(day_offset)
        except (TypeError, ValueError):
            requested_offset = 0
        if requested_offset > 0:
            requested_offset = 0
        if self._intraday_available_dates:
            requested_offset = max(requested_offset, -(len(self._intraday_available_dates) - 1))

        self._intraday_request_code = code
        self._intraday_request_offset = requested_offset
        self._intraday_day_offset = requested_offset
        self.intraday_title_var.set(f"分时图 - {code}")
        self.intraday_day_var.set("交易日: 加载中...")
        self._refresh_intraday_nav_buttons()
        self._draw_intraday_loading(f"正在加载 {code} 分时...")
        self.notebook.select(self.intraday_tab)

        normalized_target_date = str(target_trade_date or "").strip()
        self._intraday_request_target_date = normalized_target_date
        if (
            self._intraday_loading_code == code
            and self._intraday_loading_offset == requested_offset
            and self._intraday_loading_target_date == normalized_target_date
        ):
            return
        self._intraday_loading_code = code
        self._intraday_loading_offset = requested_offset
        self._intraday_loading_target_date = normalized_target_date
        threading.Thread(target=self._load_intraday, args=(code, requested_offset, normalized_target_date), daemon=True).start()

    def _load_intraday(self, stock_code: str, day_offset: int, target_trade_date: str = ""):
        try:
            payload = self.stock_filter.get_stock_intraday(
                stock_code,
                day_offset=day_offset,
                target_trade_date=target_trade_date,
            )
            self.root.after(0, lambda: self._apply_intraday_if_current(stock_code, day_offset, target_trade_date, payload))
        except Exception as e:
            self.root.after(0, lambda: self._draw_intraday_error(stock_code, f"分时加载失败: {e}"))
            self.root.after(0, lambda: self._log(f"分时加载失败 {stock_code}: {e}"))
        finally:
            self.root.after(0, lambda: self._finish_intraday_status(stock_code, day_offset))

    def _apply_intraday_if_current(self, stock_code: str, day_offset: int, target_trade_date: str, payload: Dict[str, Any]) -> None:
        code = str(stock_code).strip().zfill(6)
        if (
            code != self._intraday_request_code
            or int(day_offset) != int(self._intraday_request_offset)
            or str(target_trade_date or "").strip() != self._intraday_request_target_date
        ):
            return
        intraday_df = payload.get("intraday")
        prev_close = payload.get("prev_close")
        auction_snapshot = payload.get("auction")
        selected_trade_date = str(payload.get("selected_trade_date") or "")
        available_trade_dates = [str(d) for d in (payload.get("available_trade_dates") or [])]
        try:
            applied_day_offset = int(payload.get("applied_day_offset") or 0)
        except (TypeError, ValueError):
            applied_day_offset = int(day_offset)
        self._intraday_available_dates = available_trade_dates
        self._intraday_day_offset = applied_day_offset
        self._intraday_request_offset = applied_day_offset
        self._intraday_selected_date = selected_trade_date
        self.intraday_day_var.set(f"交易日: {selected_trade_date or '-'}")
        # 调试日志：检查分时数据内容和竞价点
        try:
            if intraday_df is not None and not intraday_df.empty:
                times = pd.to_datetime(intraday_df["time"], errors="coerce")
                time_strs = [t.strftime("%H:%M") for t in times if not pd.isna(t)]
                first_label = time_strs[0] if time_strs else "-"
                last_label = time_strs[-1] if time_strs else "-"
                has_auction = isinstance(auction_snapshot, dict) and auction_snapshot.get("price") is not None
                self._log(
                    f"【分时调试】{code} 数据共 {len(intraday_df)} 行，区间 {first_label}~{last_label}，竞价标记: {'是' if has_auction else '否'}"
                )
                if has_auction:
                    self._log(
                        f"   竞价数据: 时间={auction_snapshot.get('time')}, 价格={auction_snapshot.get('price')}, 成交量={auction_snapshot.get('volume')}"
                    )
            else:
                self._log(f"【分时调试】{code} 分时数据为空")
        except Exception as e:
            self._log(f"【分时调试】记录日志出错: {e}")

        self._refresh_intraday_nav_buttons()
        self._draw_intraday_chart(code, intraday_df, prev_close=prev_close, auction_snapshot=auction_snapshot)

    def _finish_intraday_status(self, stock_code: str, day_offset: int) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self._intraday_loading_code and int(day_offset) == int(self._intraday_loading_offset):
            self._intraday_loading_code = ""
            self._intraday_loading_offset = 0
            self._intraday_loading_target_date = ""

    def _draw_intraday_loading(self, message: str):
        self.intraday_price_ax.clear()
        self.intraday_volume_ax.clear()
        self.intraday_dist_ax.clear()
        self.intraday_price_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=13)
        self.intraday_volume_ax.text(0.5, 0.5, "请稍候", ha="center", va="center", fontsize=11)
        self.intraday_dist_ax.text(0.5, 0.5, "等待分时数据", ha="center", va="center", fontsize=11)
        self.intraday_price_ax.set_axis_off()
        self.intraday_volume_ax.set_axis_off()
        self.intraday_dist_ax.set_axis_off()
        self.intraday_canvas.draw()
    def _draw_intraday_error(self, stock_code: str, message: str):
        self.intraday_title_var.set(f"分时图 - {str(stock_code).strip().zfill(6)}")
        if not self._intraday_selected_date:
            self.intraday_day_var.set("交易日: -")
        self.intraday_price_ax.clear()
        self.intraday_volume_ax.clear()
        self.intraday_dist_ax.clear()
        self.intraday_price_ax.text(0.5, 0.5, "分时数据加载失败", ha="center", va="center", fontsize=13, color="#b22222")
        self.intraday_volume_ax.text(0.5, 0.5, message, ha="center", va="center", fontsize=10)
        self.intraday_dist_ax.text(0.5, 0.5, "无分布数据", ha="center", va="center", fontsize=10)
        self.intraday_price_ax.set_axis_off()
        self.intraday_volume_ax.set_axis_off()
        self.intraday_dist_ax.set_axis_off()
        self._refresh_intraday_nav_buttons()
        self.intraday_canvas.draw()

    def _resolve_intraday_base_price(self, close_series, prev_close: Optional[float]) -> float:
        if prev_close is not None and pd.notna(prev_close) and float(prev_close) > 0:
            return float(prev_close)
        first_close = pd.to_numeric(close_series, errors="coerce").dropna()
        if not first_close.empty:
            return max(float(first_close.iloc[0]), 1.0)
        return 1.0

    def _resolve_intraday_average_price(self, df, close_series, volume_series):
        avg_price = pd.to_numeric(df.get("avg_price"), errors="coerce")
        if not avg_price.isna().all():
            return avg_price
        cumulative_volume = volume_series.cumsum()
        if (cumulative_volume > 0).any():
            weighted_amount = (close_series.ffill().fillna(0) * volume_series).cumsum()
            return pd.to_numeric(weighted_amount / cumulative_volume.replace(0, pd.NA), errors="coerce")
        return close_series.expanding(min_periods=1).mean()

    def _normalize_intraday_auction_snapshot(self, auction_snapshot: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        context = {
            "time_label": "",
            "price": None,
            "amount": None,
            "volume": None,
            "has_auction": False,
            "x": None,
        }
        if not isinstance(auction_snapshot, dict):
            return context

        auction_time = pd.to_datetime(auction_snapshot.get("time"), errors="coerce")
        if not pd.isna(auction_time):
            context["time_label"] = auction_time.strftime("%H:%M")

        for source_key, target_key in [("price", "price"), ("amount", "amount"), ("volume", "volume")]:
            raw_value = pd.to_numeric(pd.Series([auction_snapshot.get(source_key)]), errors="coerce").iloc[0]
            if pd.notna(raw_value) and float(raw_value) > 0:
                context[target_key] = float(raw_value)

        context["has_auction"] = context["price"] is not None
        if context["has_auction"]:
            context["x"] = -0.6
        return context

    def _build_intraday_tick_positions(self, time_labels: List[str], x: List[int], auction_context: Dict[str, Any]):
        key_times = ["09:30", "10:30", "11:30", "13:00", "14:00", "15:00"]
        tick_map: Dict[int, str] = {}
        for idx, text in enumerate(time_labels):
            if text in key_times and idx not in tick_map:
                tick_map[idx] = text

        if x:
            tick_map[0] = time_labels[0]
            tick_map[len(x) - 1] = time_labels[-1]

        raw_tick_positions = sorted(tick_map.keys())
        tick_positions: List[Any] = []
        min_tick_gap = max(12, len(x) // 10) if x else 12
        for pos in raw_tick_positions:
            if not tick_positions or pos - tick_positions[-1] >= min_tick_gap or pos == len(x) - 1:
                tick_positions.append(pos)
        tick_labels = [tick_map[pos] for pos in tick_positions]

        if len(tick_positions) < 5 and x:
            tick_step = max(1, len(x) // 6)
            tick_positions = x[::tick_step]
            if tick_positions[-1] != x[-1]:
                tick_positions.append(x[-1])
            tick_labels = [time_labels[pos] for pos in tick_positions]

        if auction_context["has_auction"] and auction_context["x"] is not None:
            tick_positions = [auction_context["x"]] + tick_positions
            tick_labels = [auction_context["time_label"] or "09:25"] + tick_labels
        return tick_positions, tick_labels

    def _draw_intraday_price_panel(
        self,
        x: List[int],
        pct_close,
        pct_avg,
        pct_ma5,
        base_price: float,
        first_close,
        auction_context: Dict[str, Any],
    ) -> None:
        self.intraday_price_ax.plot(x, pct_close, color="#2f6fd6", linewidth=1.4, label="分时")
        self.intraday_price_ax.plot(x, pct_avg, color="#f08a24", linewidth=1.3, label="均价线")
        self.intraday_price_ax.plot(x, pct_ma5, color="#7b52ab", linewidth=1.0, linestyle="--", alpha=0.85, label="MA5")
        self.intraday_price_ax.axhline(0.0, color="#888888", linewidth=0.9, linestyle="--", alpha=0.85, label="昨收")
        self.intraday_price_ax.grid(True, alpha=0.25)
        self.intraday_price_ax.set_ylabel("涨\n跌\n幅\n(%)", rotation=0, labelpad=14, va="center")
        self.intraday_price_ax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.1f}%"))

        valid_pct_parts = [pct_close, pct_avg, pct_ma5]
        if auction_context["has_auction"] and auction_context["price"] is not None:
            valid_pct_parts.append(pd.Series([(auction_context["price"] / base_price - 1.0) * 100.0]))
        valid_pct = pd.concat(valid_pct_parts, ignore_index=True)
        valid_pct = pd.to_numeric(valid_pct, errors="coerce").dropna()
        if valid_pct.empty:
            self.intraday_price_ax.set_ylim(-2.0, 2.0)
        elif not first_close.empty:
            low = float(valid_pct.quantile(0.01))
            high = float(valid_pct.quantile(0.99))
            if low > high:
                low, high = high, low
            span = max(high - low, 0.4)
            pad = max(span * 0.12, 0.08)
            low = max(low - pad, -35.0)
            high = min(high + pad, 35.0)
            if low >= 0:
                low = max(0.0, low - max(span * 0.04, 0.03))
            if high <= 0:
                high = min(0.0, high + max(span * 0.04, 0.03))
            if abs(high - low) < 0.2:
                mid = (high + low) / 2.0
                low, high = mid - 0.1, mid + 0.1
            self.intraday_price_ax.set_ylim(low, high)
        else:
            self.intraday_price_ax.set_ylim(-2.0, 2.0)

        secax = self.intraday_price_ax.secondary_yaxis(
            "right",
            functions=(
                lambda y: base_price * (1.0 + y / 100.0),
                lambda p: (p / base_price - 1.0) * 100.0,
            ),
        )
        secax.set_ylabel("价\n格\n(元)", rotation=0, labelpad=12, va="center")
        secax.yaxis.set_major_formatter(FuncFormatter(lambda y, _: f"{y:.2f}"))

        self.intraday_price_ax.set_title("分时走势（含竞价标记）" if auction_context["has_auction"] else "分时走势")
        self.intraday_price_ax.legend(loc="upper right", fontsize=8, framealpha=0.85)

        if auction_context["has_auction"] and auction_context["x"] is not None and auction_context["price"] is not None:
            q_pct = (auction_context["price"] / base_price - 1.0) * 100.0
            self.intraday_price_ax.axvline(auction_context["x"] + 0.1, color="#888888", linewidth=0.9, alpha=0.7, linestyle=":")
            self.intraday_price_ax.scatter([auction_context["x"]], [q_pct], s=26, color="#555555", zorder=5, label="_nolegend_")
            first_intraday_pct = pd.to_numeric(pd.Series([pct_close.iloc[0]]), errors="coerce").iloc[0] if len(pct_close) else None
            if first_intraday_pct is not None and pd.notna(first_intraday_pct):
                self.intraday_price_ax.plot(
                    [auction_context["x"], 0],
                    [q_pct, float(first_intraday_pct)],
                    color="#777777",
                    linewidth=0.9,
                    linestyle=":",
                )
            self.intraday_price_ax.text(
                auction_context["x"],
                self.intraday_price_ax.get_ylim()[1],
                "竞价",
                ha="center",
                va="bottom",
                fontsize=9,
                color="#666666",
            )

        auction_info_text = "竞价: 无可靠数据"
        if auction_context["has_auction"] and auction_context["price"] is not None:
            parts = [auction_context["time_label"] or "09:25", f"{auction_context['price']:.2f}"]
            if auction_context["volume"] is not None:
                parts.append(f"量 {self._format_volume(auction_context['volume'])}")
            elif auction_context["amount"] is not None:
                parts.append(f"额 {self._format_amount(auction_context['amount'])}")
            auction_info_text = "竞价: " + " / ".join(parts)
        self.intraday_price_ax.text(
            0.995,
            0.98,
            auction_info_text,
            transform=self.intraday_price_ax.transAxes,
            ha="right",
            va="top",
            fontsize=8,
            color="#555555",
            bbox=dict(boxstyle="round,pad=0.2", fc="#f2f2f2", ec="#c9c9c9", alpha=0.9),
        )

    def _draw_intraday_volume_panel(
        self,
        x: List[int],
        open_series,
        close_series,
        volume_series,
        tick_positions,
        tick_labels,
        auction_context: Dict[str, Any],
    ) -> None:
        colors = [
            "#d94b4b" if (not pd.isna(c) and not pd.isna(o) and c >= o) else "#1f8b4c"
            for o, c in zip(open_series, close_series)
        ]
        self.intraday_volume_ax.bar(x, volume_series, width=0.65, color=colors, alpha=0.85)
        self.intraday_volume_ax.grid(True, alpha=0.2)
        self.intraday_volume_ax.set_ylabel("成\n交\n量", rotation=0, labelpad=10, va="center")
        self.intraday_volume_ax.set_xlabel("时间")
        if auction_context["has_auction"] and auction_context["x"] is not None:
            self.intraday_volume_ax.axvline(auction_context["x"] + 0.1, color="#888888", linewidth=0.9, alpha=0.65, linestyle=":")

        self.intraday_price_ax.set_xticks(tick_positions)
        self.intraday_price_ax.set_xticklabels([])
        self.intraday_price_ax.tick_params(axis="x", which="both", length=0)

        self.intraday_volume_ax.set_xticks(tick_positions)
        self.intraday_volume_ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)
        if x:
            left_limit = -1.0 if auction_context["has_auction"] else -0.5
            right_limit = x[-1] + 0.5
            self.intraday_price_ax.set_xlim(left_limit, right_limit)
            self.intraday_volume_ax.set_xlim(left_limit, right_limit)

    def _draw_intraday_distribution_panel(self, close_series, volume_series) -> None:
        dist_df = pd.DataFrame({"price": close_series, "volume": volume_series}).dropna(subset=["price"])
        dist_df["volume"] = pd.to_numeric(dist_df["volume"], errors="coerce").fillna(0)
        dist_df = dist_df[dist_df["volume"] > 0]

        if dist_df.empty:
            self.intraday_dist_ax.text(0.5, 0.5, "暂无成交分布数据", ha="center", va="center", fontsize=11)
            self.intraday_dist_ax.set_axis_off()
            return

        dist_df["price"] = dist_df["price"].round(2)
        grouped = dist_df.groupby("price", as_index=False)["volume"].sum()
        total_volume = float(grouped["volume"].sum())
        if total_volume <= 0:
            self.intraday_dist_ax.text(0.5, 0.5, "暂无成交分布数据", ha="center", va="center", fontsize=11)
            self.intraday_dist_ax.set_axis_off()
            return

        grouped["ratio"] = grouped["volume"] / total_volume
        grouped = grouped.sort_values("ratio", ascending=False).reset_index(drop=True).head(min(12, len(grouped)))
        y = list(range(len(grouped)))
        ratios_pct = (grouped["ratio"] * 100.0).tolist()
        labels = [f"{p:.2f}" for p in grouped["price"].tolist()]
        self.intraday_dist_ax.barh(y, ratios_pct, color="#5b7bd5", alpha=0.9)
        self.intraday_dist_ax.set_yticks(y)
        self.intraday_dist_ax.set_yticklabels(labels, fontsize=9)
        self.intraday_dist_ax.invert_yaxis()
        self.intraday_dist_ax.yaxis.tick_right()
        self.intraday_dist_ax.tick_params(axis="y", labelright=True, labelleft=False, pad=4)
        self.intraday_dist_ax.set_xlabel("占比(%)")
        self.intraday_dist_ax.set_ylabel("价\n位\n(元)", rotation=0, labelpad=14, va="center")
        self.intraday_dist_ax.yaxis.set_label_position("right")
        self.intraday_dist_ax.set_title("成交价格分布")
        self.intraday_dist_ax.grid(True, axis="x", alpha=0.2)

        for yi, value in zip(y, ratios_pct):
            text_x = max(value - 0.35, 0.12)
            text_color = "white" if value >= 3.0 else "#222222"
            self.intraday_dist_ax.text(
                text_x,
                yi,
                f"{value:.2f}%",
                va="center",
                ha="right",
                fontsize=8,
                color=text_color,
            )

    def _draw_intraday_chart(
        self,
        stock_code: str,
        intraday_df,
        prev_close: Optional[float] = None,
        auction_snapshot: Optional[Dict[str, Any]] = None,
    ):
        code = str(stock_code).strip().zfill(6)
        self.intraday_title_var.set(f"分时图 - {code}")
        self.intraday_price_ax.clear()
        self.intraday_volume_ax.clear()
        self.intraday_dist_ax.clear()
        self.intraday_price_ax.set_axis_on()
        self.intraday_volume_ax.set_axis_on()
        self.intraday_dist_ax.set_axis_on()

        if intraday_df is None or getattr(intraday_df, "empty", True):
            self._draw_intraday_error(code, "暂无分时数据")
            return

        df = intraday_df.copy().reset_index(drop=True)
        close_series = pd.to_numeric(df.get("close"), errors="coerce")
        open_series = pd.to_numeric(df.get("open"), errors="coerce")
        volume_series = pd.to_numeric(df.get("volume"), errors="coerce").fillna(0)
        times = pd.to_datetime(df.get("time"), errors="coerce")

        if close_series.isna().all() or times.isna().all():
            self._draw_intraday_error(code, "分时数据无有效价格")
            return

        base_price = self._resolve_intraday_base_price(close_series, prev_close)
        first_close = close_series.dropna()
        avg_price = self._resolve_intraday_average_price(df, close_series, volume_series)
        ma5 = close_series.rolling(window=5, min_periods=1).mean()
        auction_context = self._normalize_intraday_auction_snapshot(auction_snapshot)
        x = list(range(len(df)))
        pct_close = (close_series / base_price - 1.0) * 100.0
        pct_avg = (avg_price / base_price - 1.0) * 100.0
        pct_ma5 = (ma5 / base_price - 1.0) * 100.0
        time_labels = [t.strftime("%H:%M") if not pd.isna(t) else "" for t in times]
        self._draw_intraday_price_panel(
            x,
            pct_close,
            pct_avg,
            pct_ma5,
            base_price,
            first_close,
            auction_context,
        )
        tick_positions, tick_labels = self._build_intraday_tick_positions(time_labels, x, auction_context)
        self._draw_intraday_volume_panel(
            x,
            open_series,
            close_series,
            volume_series,
            tick_positions,
            tick_labels,
            auction_context,
        )
        self._draw_intraday_distribution_panel(close_series, volume_series)
        self.intraday_fig.tight_layout(rect=[0.02, 0.06, 0.98, 0.98], h_pad=1.2, w_pad=0.8)
        self.intraday_canvas.draw()
    def export_results(self):
        if not self.filtered_stocks:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")],
        )

        if not file_path:
            return

        try:
            with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(["代码", "名称", "板块", "最新日期", "最新收盘", "MA", "5日涨幅", "放量倍数", "放量", "涨停", "最近收盘", "结论"])
                for result in self.filtered_stocks:
                    data = result.get("data", {}) or {}
                    analysis = data.get("analysis") or {}
                    recent = analysis.get("recent_closes") or []
                    writer.writerow(
                        [
                            result.get("code", ""),
                            result.get("name", ""),
                            data.get("board", data.get("exchange", "")),
                            analysis.get("latest_date", ""),
                            "" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}",
                            "" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}",
                            "" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%",
                            "" if analysis.get("volume_expand_ratio") is None else f"{analysis['volume_expand_ratio']:.2f}x",
                            "是" if analysis.get("volume_expand") else "否",
                            "是" if analysis.get("limit_up") else "否",
                            ", ".join("" if v is None else f"{v:.2f}" for v in recent),
                            analysis.get("summary", ""),
                        ]
                    )
            messagebox.showinfo("成功", f"结果已导出到 {file_path}")
            self._log(f"结果已导出到 {file_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {e}")

    def export_results_image(self):
        if not self.filtered_stocks:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        export_columns: List[tuple[str, str]] = [("code", "代码"), ("name", "名称")]

        file_path = filedialog.asksaveasfilename(
            defaultextension=".png",
            filetypes=[("PNG图片", "*.png"), ("所有文件", "*.*")],
        )

        if not file_path:
            return

        rows = []
        for result in self.filtered_stocks:
            row_values = self._format_result_row_values(result)
            rows.append([str(row_values.get(col, "-")) for col, _ in export_columns])

        if not rows:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        output_path = Path(file_path)
        headings = [heading for _, heading in export_columns]
        column_widths = [max(self.result_headings.get(col, ("", 100))[1], 80) for col, _ in export_columns]
        total_width = sum(column_widths) or 1
        normalized_widths = [width / total_width for width in column_widths]
        exported_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            figure_width = min(max(total_width / 85, 7.2), 14.0)
            figure_height = max(4.8, 1.6 + len(rows) * 0.34)

            fig = Figure(figsize=(figure_width, figure_height), dpi=180)
            fig.patch.set_facecolor("white")
            ax = fig.add_subplot(111)
            ax.axis("off")

            fig.text(0.01, 0.985, "扫描结果导出", ha="left", va="top", fontsize=16, fontweight="bold")
            fig.text(
                0.01,
                0.957,
                f"导出时间：{exported_at}    结果数量：{len(rows)}    显示列：{len(export_columns)}",
                ha="left",
                va="top",
                fontsize=9.5,
                color="#4b5563",
            )

            table = ax.table(
                cellText=rows,
                colLabels=headings,
                colLoc="center",
                cellLoc="center",
                colWidths=normalized_widths,
                loc="upper left",
                bbox=[0, 0, 1, 0.92],
            )
            table.auto_set_font_size(False)
            table.set_fontsize(9.2)

            left_aligned_columns = {"name"}
            for (row_index, col_index), cell in table.get_celld().items():
                cell.set_edgecolor("#d7deea")
                cell.set_linewidth(0.6)
                cell.get_text().set_wrap(True)
                if row_index == 0:
                    cell.set_facecolor("#eaf2ff")
                    cell.set_text_props(weight="bold", color="#111827")
                    cell.set_height(0.042)
                else:
                    cell.set_facecolor("#ffffff" if row_index % 2 else "#f8fafc")
                    cell.set_height(0.037)
                    if export_columns[col_index][0] in left_aligned_columns:
                        cell.set_text_props(ha="left")

            fig.savefig(output_path, bbox_inches="tight", facecolor=fig.get_facecolor())
            plt.close(fig)
            messagebox.showinfo("成功", f"结果图片已导出到 {output_path}")
            self._log(f"结果图片已导出到 {output_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出图片失败: {e}")

    def copy_selected_stock_code_name(self, event=None):
        selection = self._get_selected_result_identity()
        if selection is None:
            messagebox.showwarning("提示", "请先在结果表中选中一只股票")
            return "break" if event is not None else None

        stock_code, _ = selection
        payload = stock_code

        try:
            self.root.clipboard_clear()
            self.root.clipboard_append(payload)
            self.root.update_idletasks()
            self.status_var.set(f"已复制: {payload}")
            self._log(f"已复制股票代码: {payload}")
        except tk.TclError as e:
            messagebox.showerror("错误", f"复制失败: {e}")

        return "break" if event is not None else None

    def show_settings(self):
        settings_window = tk.Toplevel(self.root)
        settings_window.title("扫描参数")
        settings_window.geometry("560x520")
        settings_window.transient(self.root)
        settings_window.grab_set()

        frame = ttk.Frame(settings_window, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="扫描数量(0=全量):").grid(row=0, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.scan_count_var, width=15).grid(row=0, column=1, pady=8)

        ttk.Label(frame, text="并发线程:").grid(row=1, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.scan_workers_var, width=15).grid(row=1, column=1, pady=8)

        ttk.Label(frame, text="连续天数:").grid(row=2, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.trend_days_var, width=15).grid(row=2, column=1, pady=8)

        ttk.Label(frame, text="MA周期:").grid(row=3, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.ma_period_var, width=15).grid(row=3, column=1, pady=8)

        ttk.Label(frame, text="近N日涨停:").grid(row=4, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.limit_up_lookback_var, width=15).grid(row=4, column=1, pady=8)

        ttk.Label(frame, text="放量观察天数:").grid(row=5, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.volume_lookback_var, width=15).grid(row=5, column=1, pady=8)

        ttk.Checkbutton(frame, text="启用放量倍数", variable=self.volume_expand_enabled_var).grid(
            row=6, column=0, columnspan=2, pady=8
        )

        ttk.Label(frame, text="放量倍数阈值:").grid(row=7, column=0, sticky=tk.E, pady=8)
        ttk.Entry(frame, textvariable=self.volume_expand_factor_var, width=15).grid(row=7, column=1, pady=8)

        ttk.Label(frame, text="备注：放量倍数=最近N天最大成交量/最小成交量").grid(
            row=8, column=0, columnspan=2, pady=8
        )

        ttk.Checkbutton(frame, text="仅显示近N日内有涨停", variable=self.require_limit_up_var).grid(
            row=9, column=0, columnspan=2, pady=8
        )

        ttk.Checkbutton(frame, text="重新拉取股票池", variable=self.refresh_universe_var).grid(
            row=10, column=0, columnspan=2, pady=8
        )

        ttk.Label(frame, text="历史数据源:").grid(row=11, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.history_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["history"],
        ).grid(row=11, column=1, pady=8)

        ttk.Label(frame, text="分时数据源:").grid(row=12, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.intraday_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["intraday"],
        ).grid(row=12, column=1, pady=8)

        ttk.Label(frame, text="资金流数据源:").grid(row=13, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.fund_flow_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["fund_flow"],
        ).grid(row=13, column=1, pady=8)

        ttk.Label(frame, text="涨停原因源:").grid(row=14, column=0, sticky=tk.E, pady=8)
        ttk.Combobox(
            frame,
            textvariable=self.limit_up_reason_source_var,
            width=15,
            state="readonly",
            values=DATA_SOURCE_OPTIONS["limit_up_reason"],
        ).grid(row=14, column=1, pady=8)

        ttk.Button(
            frame,
            text="保存",
            command=lambda: (self._save_app_settings(), self._apply_source_preferences(), settings_window.destroy()),
        ).grid(
            row=15, column=0, columnspan=2, pady=18
        )

    def show_about(self):
        messagebox.showinfo(
            "关于",
            "日终股票筛选器\n\n"
            "功能:\n"
            "- 只使用历史日线数据\n"
            "- 筛选最近N日收盘全部高于MA\n"
            "- 可过滤近N日内出现过涨停的股票\n"
            "- 结果和历史数据都会保存到 data/stock_store.sqlite3\n",
        )

    def on_clear_universe_data(self):
        clear_universe_data()
        self._log("已清空股票池和结果快照。")

    def on_clear_history_data(self):
        clear_history_data()
        self._log("已清空历史数据。")

    def on_close(self):
        self._is_closing = True
        self.is_scanning = False
        self.is_updating_cache = False
        self._detail_request_code = ""
        self._detail_loading_code = ""
        self._intraday_request_code = ""
        self._intraday_loading_code = ""
        self._cancel_scheduled_detail()
        self._close_run_log()
        try:
            self.root.quit()
            self.root.destroy()
        except Exception:
            pass


def run_app():
    root = tk.Tk()
    app = StockMonitorApp(root)
    root.mainloop()


if __name__ == "__main__":
    run_app()














