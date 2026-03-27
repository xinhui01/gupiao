from __future__ import annotations

import csv
import json
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
from tkinter import ttk, messagebox, scrolledtext

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
        self.root.geometry("1680x980")
        self.root.minsize(1280, 820)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        try:
            self.root.state("zoomed")
        except tk.TclError:
            pass

        self.stock_filter = StockFilter()
        self.stock_filter.set_log_callback(self._log_async)
        self.all_scan_results: List[Dict[str, Any]] = []
        self.filtered_stocks: List[Dict[str, Any]] = []
        self._detail_request_code = ""
        self._detail_loading_code = ""
        self._detail_after_id = None
        self.sort_column = "five_day_return"
        self.sort_reverse = True
        self.is_scanning = False
        self._run_log_file: Optional[Path] = None
        self._current_scan_allowed_boards: List[str] = []
        self._current_scan_max_stocks: int = 0
        self.result_columns: tuple[str, ...] = ()
        self.result_headings: Dict[str, tuple[str, int]] = {}
        self.result_column_vars: Dict[str, tk.BooleanVar] = {}
        self.result_column_order: List[str] = []
        self.default_result_display_columns: tuple[str, ...] = ()
        self.result_layout_path = Path("data") / "result_columns.json"

        self.setup_ui()
        self._load_result_column_layout()
        self.apply_result_display_columns(save=False)
        self._load_last_results()

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
        file_menu.add_command(label="导出结果", command=self.export_results)
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

    def setup_control_panel(self, parent):
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding="10")
        control_frame.pack(fill=tk.X, pady=5)

        row1 = ttk.Frame(control_frame)
        row1.pack(fill=tk.X, pady=5)

        ttk.Label(row1, text="扫描数量(0=全量):").pack(side=tk.LEFT, padx=5)
        self.scan_count_var = tk.StringVar(value="0")
        ttk.Entry(row1, textvariable=self.scan_count_var, width=8).pack(side=tk.LEFT, padx=5)

        ttk.Label(row1, text="并发线程:").pack(side=tk.LEFT, padx=5)
        self.scan_workers_var = tk.StringVar(value="12")
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

        row2 = ttk.Frame(control_frame)
        row2.pack(fill=tk.X, pady=5)

        self.scan_btn = ttk.Button(row2, text="开始扫描", command=self.start_scan)
        self.scan_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(row2, text="停止", command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        ttk.Label(row2, text="股票代码:").pack(side=tk.LEFT, padx=5)
        self.stock_code_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self.stock_code_var, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(row2, text="查询股票", command=self.query_single_stock).pack(side=tk.LEFT, padx=15)
        ttk.Button(row2, text="列表列设置", command=self.show_column_picker).pack(side=tk.LEFT, padx=8)

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

    def setup_notebook(self, parent):
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)

        self.setup_result_tab()
        self.setup_detail_tab()
        self.setup_log_tab()

    def setup_result_tab(self):
        result_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(result_frame, text="扫描结果")

        self.result_columns = (
            "code",
            "name",
            "board",
            "concepts",
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
            "limit_up_reason",
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
            "concepts": ("概念", 220),
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
            "limit_up_reason": ("涨停原因", 200),
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

        info_frame = ttk.LabelFrame(detail_frame, text="历史摘要", padding="10")
        info_frame.pack(fill=tk.X, pady=5)

        self.detail_labels: Dict[str, ttk.Label] = {}
        items = [
            ("code", "股票代码"),
            ("name", "股票名称"),
            ("concepts", "概念"),
            ("latest_date", "最新日期"),
            ("quote_time", "刷新时间"),
            ("latest_close", "最新收盘"),
            ("latest_ma", "MA"),
            ("latest_ma10", "MA10"),
            ("latest_volume", "成交量"),
            ("latest_amount", "成交额"),
            ("five_day_return", "5日涨幅"),
            ("limit_up", "涨停"),
            ("limit_up_reason", "涨停原因"),
            ("volume_expand", "放量"),
            ("volume_expand_ratio", "放量倍数"),
            ("big_order_amount", "大单净额"),
            ("main_force_amount", "主力净额"),
            ("summary", "结论"),
        ]

        for i, (key, label) in enumerate(items):
            row = i // 3
            col = (i % 3) * 2
            ttk.Label(info_frame, text=f"{label}:").grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.detail_labels[key] = ttk.Label(info_frame, text="-", width=30)
            self.detail_labels[key].grid(row=row, column=col + 1, padx=5, pady=5, sticky=tk.W)

        chart_frame = ttk.LabelFrame(detail_frame, text="K线图", padding="5")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        self.fig, (self.price_ax, self.volume_ax, self.flow_ax) = plt.subplots(
            3,
            1,
            figsize=(11, 7.4),
            sharex=True,
            gridspec_kw={"height_ratios": [3.2, 1.2, 1.2]},
        )
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

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
        line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
        self.log_text.insert(tk.END, line)
        self.log_text.see(tk.END)
        if self._run_log_file is not None:
            with self._run_log_file.open("a", encoding="utf-8") as f:
                f.write(line)

    def _log_async(self, message: str) -> None:
        self.root.after(0, lambda: self._log(message))

    def _selected_boards(self) -> List[str]:
        boards = [board for board, var in self.board_filter_vars.items() if var.get()]
        return boards or list(self.board_filter_vars.keys())

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

    def _ensure_result_concepts(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not results:
            return results
        for result in results:
            data = result.setdefault("data", {}) or {}
            concepts = str(data.get("concepts", "") or "").strip()
            if concepts:
                continue
            code = str(result.get("code", "") or "").strip().zfill(6)
            if not code:
                continue
            concepts = self.stock_filter.fetcher.get_stock_concepts(code)
            if concepts:
                data["concepts"] = concepts
        return results

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

    def on_board_filter_changed(self):
        if self.is_scanning:
            return
        source = self.all_scan_results or self.filtered_stocks
        if not source:
            return
        filtered = self._filter_results_by_selected_boards(source)
        self.update_result_table(filtered, announce=False, persist=False)
        self.status_var.set(f"已按板块筛选，当前显示 {len(filtered)} 只")

    def _scan_signature(self, allowed_boards: List[str], max_stocks: int) -> Dict[str, Any]:
        return {
            "trend_days": int(self.stock_filter.trend_days),
            "ma_period": int(self.stock_filter.ma_period),
            "limit_up_lookback_days": int(self.stock_filter.limit_up_lookback_days),
            "volume_lookback_days": int(self.stock_filter.volume_lookback_days),
            "volume_expand_enabled": bool(self.stock_filter.volume_expand_enabled),
            "volume_expand_factor": float(self.stock_filter.volume_expand_factor),
            "require_limit_up_within_days": bool(self.stock_filter.require_limit_up_within_days),
            "allowed_boards": sorted({str(x).strip() for x in allowed_boards if str(x).strip()}),
            "max_stocks": int(max_stocks),
        }

    def _save_last_results(self, results: List[Dict[str, Any]], complete: bool = True) -> None:
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
                        "concepts": data.get("concepts", ""),
                        "analysis": analysis,
                    },
                }
            )
        payload = {
            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_date": datetime.now().strftime("%Y-%m-%d"),
            "complete": bool(complete),
            "row_count": len(payload_results),
            "signature": self._scan_signature(self._current_scan_allowed_boards, self._current_scan_max_stocks),
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
        self.filtered_stocks = results
        self.update_result_table(results, announce=False, persist=False)
        self.status_var.set("已从本地结果恢复")

    def _can_use_snapshot(self, allowed_boards: List[str], max_stocks: int) -> bool:
        if self.ignore_result_snapshot_var.get():
            return False
        if self.refresh_universe_var.get():
            return False
        signature = self._scan_signature(allowed_boards, max_stocks)
        payload = load_scan_snapshot(json.dumps(signature, ensure_ascii=False, sort_keys=True))
        return bool(payload and payload.get("complete") and payload.get("results"))

    def update_filter_params(self) -> bool:
        try:
            self.stock_filter.trend_days = max(1, int(self.trend_days_var.get()))
            self.stock_filter.ma_period = max(1, int(self.ma_period_var.get()))
            self.stock_filter.limit_up_lookback_days = max(1, int(self.limit_up_lookback_var.get()))
            self.stock_filter.volume_lookback_days = max(1, int(self.volume_lookback_var.get()))
            self.stock_filter.volume_expand_enabled = bool(self.volume_expand_enabled_var.get())
            self.stock_filter.volume_expand_factor = max(1.0, float(self.volume_expand_factor_var.get()))
            self.stock_filter.require_limit_up_within_days = bool(self.require_limit_up_var.get())
            return True
        except ValueError:
            messagebox.showerror("错误", "参数无效")
            return False

    def start_scan(self):
        if not self.update_filter_params():
            return
        if self.is_scanning:
            return

        try:
            max_stocks = int(self.scan_count_var.get())
        except ValueError:
            max_stocks = 0
        allowed_boards = self._selected_boards()
        self._current_scan_allowed_boards = allowed_boards
        self._current_scan_max_stocks = max_stocks

        if self._can_use_snapshot(allowed_boards, max_stocks):
            self._log("命中本地结果快照，直接恢复上次扫描结果。")
            signature = json.dumps(self._scan_signature(allowed_boards, max_stocks), ensure_ascii=False, sort_keys=True)
            payload = load_scan_snapshot(signature)
            if payload:
                self.all_scan_results = payload.get("results", []) or []
                self.update_result_table(self.all_scan_results, announce=False, persist=False)
                self.status_var.set("已从本地结果恢复")
                self.progress_var.set(100)
                self.scan_finished()
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
            f"开始扫描：最近{self.stock_filter.trend_days}日收盘 > MA{self.stock_filter.ma_period}，"
            f"近{self.stock_filter.limit_up_lookback_days}日内涨停过滤={'开' if self.stock_filter.require_limit_up_within_days else '关'}。"
        )
        self._log("扫描阶段只拉历史日线，不拉实时、资金流或内外盘。")
        self.status_var.set("正在扫描...")
        self.progress_var.set(0)

        scan_thread = threading.Thread(target=self.scan_stocks, daemon=True)
        scan_thread.start()

    def stop_scan(self):
        self.is_scanning = False
        self.status_var.set("正在停止...")
        self._log("已请求停止，正在等待当前任务结束。")

    def scan_stocks(self):
        try:
            try:
                max_stocks = int(self.scan_count_var.get())
            except ValueError:
                max_stocks = 0
            try:
                scan_workers = int(self.scan_workers_var.get())
            except ValueError:
                scan_workers = 12
            scan_workers = max(1, min(scan_workers, 16))

            self._log_async(
                f"扫描参数：数量={'全量' if max_stocks <= 0 else max_stocks}，并发线程={scan_workers}"
            )

            def progress_callback(current, total, code, name):
                if not self.is_scanning:
                    raise StopIteration
                progress = (current / total) * 100 if total else 0
                self.root.after(0, lambda: self.progress_var.set(progress))
                self.root.after(0, lambda: self.status_var.set(f"扫描中 {current}/{total}: {code} {name}"))

            results = self.stock_filter.scan_all_stocks(
                max_stocks=max_stocks,
                progress_callback=progress_callback,
                max_workers=scan_workers,
                should_stop=lambda: not self.is_scanning,
                refresh_universe=self.refresh_universe_var.get(),
                allowed_boards=self._selected_boards(),
            )
            self.all_scan_results = results
            self.root.after(0, lambda: self.update_result_table(results))
        except StopIteration:
            self.root.after(0, lambda: self._log("扫描已停止。"))
        except Exception as e:
            self.root.after(0, lambda: self._log(f"扫描出错: {e}"))
        finally:
            self.root.after(0, self.scan_finished)

    def _sort_value_for_column(self, item: Dict[str, Any], column: str):
        data = item.get("data", {}) or {}
        analysis = data.get("analysis") or {}

        if column == "code":
            return str(item.get("code", "")).zfill(6)
        if column == "name":
            return str(item.get("name", ""))
        if column == "board":
            return str(data.get("board") or data.get("exchange") or "")
        if column == "concepts":
            return str(data.get("concepts") or "")
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
        if column == "limit_up_reason":
            return str(analysis.get("limit_up_reason", ""))
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
    ):
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)

        results = self._filter_results_by_selected_boards(results)
        results = self._sort_results(results)

        for result in results:
            data = result.get("data", {}) or {}
            analysis = data.get("analysis") or {}
            recent = analysis.get("recent_closes") or []
            recent_text = ", ".join("-" if v is None else f"{v:.2f}" for v in recent)
            five_day_return = analysis.get("five_day_return")
            volume_expand_ratio = analysis.get("volume_expand_ratio")
            volume_expand = "是" if analysis.get("volume_expand") else "否"
            limit_up_streak = analysis.get("limit_up_streak") or 0
            broken_limit_up = "是" if analysis.get("broken_limit_up") else "否"
            volume_break_limit_up = "是" if analysis.get("volume_break_limit_up") else "否"
            after_two_limit_up = "是" if analysis.get("after_two_limit_up") else "否"
            limit_up = "是" if analysis.get("limit_up") else "否"
            values = (
                result.get("code", "-"),
                result.get("name", "-"),
                data.get("board") or data.get("exchange") or "-",
                self._short_text(data.get("concepts", "-"), 30),
                "-" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}",
                "-" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}",
                "-" if five_day_return is None else f"{five_day_return:.2f}%",
                limit_up_streak,
                broken_limit_up,
                "-" if volume_expand_ratio is None else f"{volume_expand_ratio:.2f}x",
                volume_expand,
                volume_break_limit_up,
                after_two_limit_up,
                limit_up,
                analysis.get("limit_up_reason", "-"),
                recent_text,
            )
            self.result_tree.insert("", tk.END, values=values)

        self.filtered_stocks = results
        if persist:
            self._save_last_results(results, complete=True)
        if announce:
            self._log(f"扫描完成，命中 {len(results)} 只。")

    def scan_finished(self):
        self.scan_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.status_var.set("扫描完成")
        self.is_scanning = False
        self.refresh_universe_var.set(False)
        self._close_run_log()

    def on_stock_select(self, event):
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item["values"][0]
            self._schedule_show_stock_detail(stock_code)

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
        detail_payload = None
        for result in self.filtered_stocks:
            if str(result.get("code", "")).strip().zfill(6) == code:
                data = result.get("data", {}) or {}
                detail_payload = {
                    "code": code,
                    "name": result.get("name", ""),
                    "board": data.get("board", ""),
                    "exchange": data.get("exchange", ""),
                    "concepts": data.get("concepts", ""),
                    "history": data.get("history"),
                    "analysis": data.get("analysis") or {},
                }
                break

        if detail_payload is not None:
            self._log(f"展示扫描结果中的股票 {code} 详情。")
            self._update_detail_ui(detail_payload)
            if not force_refresh and self._detail_loading_code == code:
                self.status_var.set(f"{code} 详情正在加载...")
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
        self._show_detail_loading(code)
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
        finally:
            self.root.after(0, lambda: self._finish_detail_status(stock_code))

    def _apply_detail_if_current(self, stock_code: str, detail: Dict[str, Any]) -> None:
        if str(stock_code).strip().zfill(6) != self._detail_request_code:
            return
        self._update_detail_ui(detail)

    def _finish_detail_status(self, stock_code: str) -> None:
        code = str(stock_code).strip().zfill(6)
        if code == self._detail_loading_code:
            self._detail_loading_code = ""
        if code != self._detail_request_code:
            return
        self.status_var.set("查询完成")

    def _show_detail_loading(self, stock_code: str) -> None:
        placeholders = {
            "code": str(stock_code).strip().zfill(6),
            "name": "加载中...",
            "concepts": "加载中...",
            "latest_date": "加载中...",
            "quote_time": "加载中...",
            "latest_close": "加载中...",
            "latest_ma": "加载中...",
            "latest_ma10": "加载中...",
            "latest_volume": "加载中...",
            "latest_amount": "加载中...",
            "five_day_return": "加载中...",
            "limit_up": "加载中...",
            "limit_up_reason": "加载中...",
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

    def _update_detail_ui(self, detail: Dict[str, Any]):
        analysis = detail.get("analysis") or {}
        history = detail.get("history")

        self.detail_labels["code"].config(text=detail.get("code", "-"))
        self.detail_labels["name"].config(text=detail.get("name", "-"))
        self.detail_labels["concepts"].config(text=self._short_text(detail.get("concepts", "-"), 60))
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
        self.detail_labels["latest_volume"].config(text=self._format_volume(analysis.get("latest_volume")))
        self.detail_labels["latest_amount"].config(text=self._format_amount(analysis.get("latest_amount")))
        self.detail_labels["five_day_return"].config(
            text="-" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%"
        )
        self.detail_labels["limit_up"].config(text="是" if analysis.get("limit_up") else "否")
        self.detail_labels["limit_up_reason"].config(text=analysis.get("limit_up_reason", "-"))
        self.detail_labels["big_order_amount"].config(text=self._format_amount(analysis.get("big_order_amount")))
        self.detail_labels["main_force_amount"].config(text=self._format_amount(analysis.get("main_force_amount")))
        self.detail_labels["summary"].config(text=analysis.get("summary", "-"))

        self._draw_chart(history, analysis)

    def _draw_chart(self, history, analysis):
        self.price_ax.clear()
        self.volume_ax.clear()
        self.flow_ax.clear()
        self.price_ax.set_axis_on()
        self.volume_ax.set_axis_on()
        self.flow_ax.set_axis_on()

        if history is None or getattr(history, "empty", True):
            self.price_ax.text(0.5, 0.5, "暂无历史数据", ha="center", va="center", fontsize=14)
            self.canvas.draw()
            return

        df = history.copy().tail(30).reset_index(drop=True)
        x = list(range(len(df)))
        dates = df["date"].astype(str).tolist() if "date" in df.columns else [str(i) for i in x]
        opens = pd.to_numeric(df["open"], errors="coerce") if "open" in df.columns else pd.Series([None] * len(df))
        closes = pd.to_numeric(df["close"], errors="coerce") if "close" in df.columns else pd.Series([None] * len(df))
        highs = pd.to_numeric(df["high"], errors="coerce") if "high" in df.columns else pd.Series([None] * len(df))
        lows = pd.to_numeric(df["low"], errors="coerce") if "low" in df.columns else pd.Series([None] * len(df))
        volumes = pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else pd.Series([0] * len(df))

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
        self.price_ax.plot(x, closes, color="#2f6fd6", linewidth=1.0, alpha=0.35, label="收盘价")
        self.price_ax.plot(x, ma, color="#f08a24", linewidth=1.4, label=f"MA{ma_period}")
        self.price_ax.plot(x, ma10, color="#7b52ab", linewidth=1.2, label="MA10")

        volume_colors = [
            "#d94b4b" if (not pd.isna(c) and not pd.isna(o) and c >= o) else "#1f8b4c"
            for o, c in zip(opens, closes)
        ]
        self.volume_ax.bar(x, volumes.fillna(0), width=0.6, color=volume_colors, alpha=0.85)

        flow_history = analysis.get("fund_flow_history") or []
        flow_map = {}
        for item in flow_history:
            if not isinstance(item, dict):
                continue
            flow_date = str(item.get("date", "") or "").strip()
            if not flow_date:
                continue
            flow_map[flow_date] = item
        big_order_values = []
        flow_colors = []
        for idx, date_str in enumerate(dates):
            flow_item = flow_map.get(date_str, {})
            amount = pd.to_numeric(pd.Series([flow_item.get("big_order_amount")]), errors="coerce").iloc[0]
            if pd.isna(amount):
                amount = 0.0
            big_order_values.append(float(amount))
            flow_colors.append("#d94b4b" if amount >= 0 else "#1f8b4c")
        self.flow_ax.bar(x, big_order_values, width=0.6, color=flow_colors, alpha=0.85)
        self.flow_ax.axhline(0, color="#666666", linewidth=0.8, alpha=0.6)

        tick_step = max(1, len(x) // 6)
        tick_positions = x[::tick_step]
        if x and tick_positions[-1] != x[-1]:
            tick_positions.append(x[-1])
        tick_labels = [dates[pos][5:] if len(dates[pos]) >= 10 else dates[pos] for pos in tick_positions]

        self.price_ax.set_xlim(-0.5, len(x) - 0.5)
        self.volume_ax.set_xlim(-0.5, len(x) - 0.5)
        self.flow_ax.set_xlim(-0.5, len(x) - 0.5)
        self.price_ax.set_ylabel("价格")
        self.price_ax.set_title("近一个月K线")
        self.price_ax.legend(loc="upper left")
        self.price_ax.grid(True, alpha=0.25)
        self.volume_ax.set_ylabel("成交量")
        self.volume_ax.grid(True, alpha=0.2)
        self.flow_ax.set_ylabel("大单净额")
        self.flow_ax.set_xlabel("日期")
        self.flow_ax.grid(True, alpha=0.2)
        self.price_ax.set_xticks(tick_positions)
        self.price_ax.set_xticklabels([])
        self.volume_ax.set_xticks(tick_positions)
        self.volume_ax.set_xticklabels([])
        self.volume_ax.set_xticks(tick_positions)
        self.flow_ax.set_xticks(tick_positions)
        self.flow_ax.set_xticklabels(tick_labels, rotation=45, ha="right")

        self.fig.tight_layout()
        self.canvas.draw()

    def export_results(self):
        if not self.filtered_stocks:
            messagebox.showwarning("警告", "没有可导出的结果")
            return

        from tkinter import filedialog

        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")],
        )

        if not file_path:
            return

        try:
            with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(["代码", "名称", "板块", "概念", "最新日期", "最新收盘", "MA", "5日涨幅", "放量倍数", "放量", "涨停", "涨停原因", "最近收盘", "结论"])
                for result in self.filtered_stocks:
                    data = result.get("data", {}) or {}
                    analysis = data.get("analysis") or {}
                    recent = analysis.get("recent_closes") or []
                    writer.writerow(
                        [
                            result.get("code", ""),
                            result.get("name", ""),
                            data.get("board", data.get("exchange", "")),
                            data.get("concepts", ""),
                            analysis.get("latest_date", ""),
                            "" if analysis.get("latest_close") is None else f"{analysis['latest_close']:.2f}",
                            "" if analysis.get("latest_ma") is None else f"{analysis['latest_ma']:.2f}",
                            "" if analysis.get("five_day_return") is None else f"{analysis['five_day_return']:.2f}%",
                            "" if analysis.get("volume_expand_ratio") is None else f"{analysis['volume_expand_ratio']:.2f}x",
                            "是" if analysis.get("volume_expand") else "否",
                            "是" if analysis.get("limit_up") else "否",
                            analysis.get("limit_up_reason", ""),
                            ", ".join("" if v is None else f"{v:.2f}" for v in recent),
                            analysis.get("summary", ""),
                        ]
                    )
            messagebox.showinfo("成功", f"结果已导出到 {file_path}")
            self._log(f"结果已导出到 {file_path}")
        except Exception as e:
            messagebox.showerror("错误", f"导出失败: {e}")

    def show_settings(self):
        settings_window = tk.Toplevel(self.root)
        settings_window.title("扫描参数")
        settings_window.geometry("520x340")
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

        ttk.Button(frame, text="保存", command=lambda: settings_window.destroy()).grid(
            row=11, column=0, columnspan=2, pady=18
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
        self.is_scanning = False
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
