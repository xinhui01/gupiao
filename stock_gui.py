import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
from datetime import datetime
from typing import List, Dict, Any
from stock_filter import StockFilter
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import matplotlib
matplotlib.use('TkAgg')
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False


class StockMonitorApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("股票监控程序")
        self.root.geometry("1400x900")
        self.root.minsize(1200, 800)
        
        self.stock_filter = StockFilter()
        self.stock_filter.set_log_callback(self._log_async)
        self.filtered_stocks: List[Dict[str, Any]] = []
        self.is_scanning = False
        
        self.setup_ui()
        
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
        file_menu.add_command(label="退出", command=self.root.quit)
        
        setting_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="设置", menu=setting_menu)
        setting_menu.add_command(label="筛选条件", command=self.show_settings)
        setting_menu.add_command(label="清除股票池本地缓存", command=self.on_clear_universe_cache)
        
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="帮助", menu=help_menu)
        help_menu.add_command(label="关于", command=self.show_about)
        
    def setup_control_panel(self, parent):
        control_frame = ttk.LabelFrame(parent, text="控制面板", padding="10")
        control_frame.pack(fill=tk.X, pady=5)
        
        row1 = ttk.Frame(control_frame)
        row1.pack(fill=tk.X, pady=5)
        
        ttk.Label(row1, text="量比阈值:").pack(side=tk.LEFT, padx=5)
        self.volume_ratio_var = tk.StringVar(value="3.0")
        ttk.Entry(row1, textvariable=self.volume_ratio_var, width=8).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row1, text="内外盘比阈值:").pack(side=tk.LEFT, padx=5)
        self.inner_outer_var = tk.StringVar(value="1.0")
        ttk.Entry(row1, textvariable=self.inner_outer_var, width=8).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row1, text="主力净流入(万):").pack(side=tk.LEFT, padx=5)
        self.net_inflow_var = tk.StringVar(value="0")
        ttk.Entry(row1, textvariable=self.net_inflow_var, width=10).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row1, text="连续天数(收盘>MA5):").pack(side=tk.LEFT, padx=5)
        self.trend_days_var = tk.StringVar(value="5")
        ttk.Entry(row1, textvariable=self.trend_days_var, width=5).pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row1, text="扫描数量:").pack(side=tk.LEFT, padx=5)
        self.scan_count_var = tk.StringVar(value="200")
        ttk.Entry(row1, textvariable=self.scan_count_var, width=8).pack(side=tk.LEFT, padx=5)
        ttk.Label(row1, text="并发线程:").pack(side=tk.LEFT, padx=5)
        self.scan_workers_var = tk.StringVar(value="12")
        ttk.Entry(row1, textvariable=self.scan_workers_var, width=5).pack(side=tk.LEFT, padx=5)
        
        row2 = ttk.Frame(control_frame)
        row2.pack(fill=tk.X, pady=5)
        
        self.refresh_universe_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            row2,
            text="重新拉取股票池",
            variable=self.refresh_universe_var,
        ).pack(side=tk.LEFT, padx=10)

        ttk.Label(row2, text="价格范围:").pack(side=tk.LEFT, padx=5)
        self.min_price_var = tk.StringVar(value="2.0")
        ttk.Entry(row2, textvariable=self.min_price_var, width=8).pack(side=tk.LEFT, padx=5)
        ttk.Label(row2, text="-").pack(side=tk.LEFT)
        self.max_price_var = tk.StringVar(value="100.0")
        ttk.Entry(row2, textvariable=self.max_price_var, width=8).pack(side=tk.LEFT, padx=5)
        
        self.scan_btn = ttk.Button(row2, text="开始扫描", command=self.start_scan)
        self.scan_btn.pack(side=tk.LEFT, padx=20)
        
        self.stop_btn = ttk.Button(row2, text="停止", command=self.stop_scan, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(row2, text="股票代码:").pack(side=tk.LEFT, padx=20)
        self.stock_code_var = tk.StringVar()
        ttk.Entry(row2, textvariable=self.stock_code_var, width=10).pack(side=tk.LEFT, padx=5)
        ttk.Button(row2, text="查询", command=self.query_single_stock).pack(side=tk.LEFT, padx=5)
        
    def setup_notebook(self, parent):
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.setup_result_tab()
        
        self.setup_detail_tab()
        
        self.setup_log_tab()
        
    def setup_result_tab(self):
        result_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(result_frame, text="扫描结果")
        
        columns = ('代码', '名称', '现价', '涨跌幅', '量比', '内外盘比', '主力净流入(万)', '连续涨幅%')
        self.result_tree = ttk.Treeview(result_frame, columns=columns, show='headings', height=20)
        
        col_widths = [80, 100, 80, 80, 80, 100, 120, 100]
        for col, width in zip(columns, col_widths):
            self.result_tree.heading(col, text=col)
            self.result_tree.column(col, width=width, anchor=tk.CENTER)
        
        scrollbar = ttk.Scrollbar(result_frame, orient=tk.VERTICAL, command=self.result_tree.yview)
        self.result_tree.configure(yscrollcommand=scrollbar.set)
        
        self.result_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        self.result_tree.bind('<<TreeviewSelect>>', self.on_stock_select)
        self.result_tree.bind('<Double-1>', self.on_stock_double_click)
        
    def setup_detail_tab(self):
        detail_frame = ttk.Frame(self.notebook, padding="5")
        self.notebook.add(detail_frame, text="股票详情")
        
        info_frame = ttk.LabelFrame(detail_frame, text="基本信息", padding="10")
        info_frame.pack(fill=tk.X, pady=5)
        
        self.detail_labels = {}
        detail_items = [
            ('code', '股票代码:'), ('name', '股票名称:'), ('price', '当前价格:'),
            ('change_pct', '涨跌幅:'), ('volume', '成交量:'), ('amount', '成交额:'),
            ('volume_ratio', '量比:'), ('turnover_rate', '换手率:')
        ]
        
        for i, (key, label) in enumerate(detail_items):
            row = i // 4
            col = (i % 4) * 2
            ttk.Label(info_frame, text=label).grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.detail_labels[key] = ttk.Label(info_frame, text="-", width=15)
            self.detail_labels[key].grid(row=row, column=col+1, padx=5, pady=5, sticky=tk.W)
        
        flow_frame = ttk.LabelFrame(detail_frame, text="资金流向", padding="10")
        flow_frame.pack(fill=tk.X, pady=5)
        
        flow_items = [
            ('main_net_inflow', '主力净流入:'), ('main_net_inflow_pct', '主力净占比:'),
            ('retail_net_inflow', '散户净流入:'), ('retail_net_inflow_pct', '散户净占比:')
        ]
        
        for i, (key, label) in enumerate(flow_items):
            row = i // 4
            col = (i % 4) * 2
            ttk.Label(flow_frame, text=label).grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.detail_labels[key] = ttk.Label(flow_frame, text="-", width=15)
            self.detail_labels[key].grid(row=row, column=col+1, padx=5, pady=5, sticky=tk.W)
        
        inner_outer_frame = ttk.LabelFrame(detail_frame, text="内外盘", padding="10")
        inner_outer_frame.pack(fill=tk.X, pady=5)
        
        io_items = [
            ('inner_volume', '内盘:'), ('outer_volume', '外盘:'),
            ('inner_outer_ratio', '内外盘比:'), ('outer_pct', '外盘占比:')
        ]
        
        for i, (key, label) in enumerate(io_items):
            row = i // 4
            col = (i % 4) * 2
            ttk.Label(inner_outer_frame, text=label).grid(row=row, column=col, padx=5, pady=5, sticky=tk.E)
            self.detail_labels[key] = ttk.Label(inner_outer_frame, text="-", width=15)
            self.detail_labels[key].grid(row=row, column=col+1, padx=5, pady=5, sticky=tk.W)
        
        chart_frame = ttk.LabelFrame(detail_frame, text="K线图", padding="5")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.fig, self.ax = plt.subplots(figsize=(10, 4))
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
        
    def log(self, message: str):
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{ts}] {message}\n")
        self.log_text.see(tk.END)

    def on_clear_universe_cache(self) -> None:
        from stock_data import clear_universe_disk_cache

        clear_universe_disk_cache()
        self.log("已清除股票池本地缓存（cache/a_share_universe.csv），下次扫描将从交易所重新拉列表。")

    def _log_async(self, message: str) -> None:
        """供后台线程调用，安全刷到「运行日志」页。"""
        self.root.after(0, lambda m=message: self.log(m))
        
    def update_filter_params(self):
        try:
            self.stock_filter.min_volume_ratio = float(self.volume_ratio_var.get())
            self.stock_filter.min_inner_outer_ratio = float(self.inner_outer_var.get())
            self.stock_filter.min_main_net_inflow = float(self.net_inflow_var.get())
            self.stock_filter.trend_days = int(self.trend_days_var.get())
            self.stock_filter.min_price = float(self.min_price_var.get())
            self.stock_filter.max_price = float(self.max_price_var.get())
            return True
        except ValueError:
            messagebox.showerror("错误", "请输入有效的数值")
            return False
            
    def start_scan(self):
        if not self.update_filter_params():
            return
            
        self.is_scanning = True
        self.scan_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)
        self.filtered_stocks.clear()
        
        self.log("开始扫描股票…")
        self.stock_filter.fetcher.clear_quote_cache()
        self._log_async("已提交后台扫描线程，随后会显示各阶段进度。")
        self.status_var.set("正在扫描...")
        
        scan_thread = threading.Thread(target=self.scan_stocks, daemon=True)
        scan_thread.start()
        
    def stop_scan(self):
        self.is_scanning = False
        self.status_var.set("已停止")
        
    def scan_stocks(self):
        self._log_async("扫描线程开始执行…")
        try:
            max_stocks = int(self.scan_count_var.get())
        except ValueError:
            max_stocks = 200
        try:
            scan_workers = int(self.scan_workers_var.get())
        except ValueError:
            scan_workers = 12
        scan_workers = max(1, min(scan_workers, 64))
        self._log_async(
            f"本次最多检测 {max_stocks} 只股票，并发线程 {scan_workers}（过大易被限流）。"
        )

        def progress_callback(current, total, code, name):
            if not self.is_scanning:
                raise StopIteration
            progress = (current / total) * 100
            self.root.after(0, lambda: self.progress_var.set(progress))
            self.root.after(0, lambda: self.status_var.set(f"扫描中: {current}/{total} - {code} {name}"))
        
        try:
            results = self.stock_filter.scan_all_stocks(
                max_stocks=max_stocks,
                progress_callback=progress_callback,
                max_workers=scan_workers,
                should_stop=lambda: not self.is_scanning,
                refresh_universe=self.refresh_universe_var.get(),
            )
            self.filtered_stocks = results
            self.root.after(0, lambda: self.update_result_table(results))
        except StopIteration:
            self.root.after(0, lambda: self.log("扫描已停止"))
        except Exception as e:
            self.root.after(0, lambda: self.log(f"扫描出错: {e}"))
        finally:
            self.root.after(0, self.scan_finished)
            
    def update_result_table(self, results: List[Dict[str, Any]]):
        for item in self.result_tree.get_children():
            self.result_tree.delete(item)
            
        for result in results:
            data = result.get('data', {})
            realtime = data.get('realtime') or {}
            flow = data.get('flow') or {}
            inner_outer = data.get('inner_outer') or {}
            history = data.get('history')
            
            trend_change = 0
            nd = self.stock_filter.trend_days
            if history is not None and len(history) >= nd:
                recent = history.tail(nd)
                if 'change_pct' in recent.columns:
                    trend_change = recent['change_pct'].sum()
            
            values = (
                result['code'],
                realtime.get('name', '-'),
                f"{realtime.get('price', 0):.2f}",
                f"{realtime.get('change_pct', 0):.2f}%",
                f"{realtime.get('volume_ratio', 0):.2f}",
                f"{inner_outer.get('inner_outer_ratio', 0):.2f}",
                f"{flow.get('main_net_inflow', 0):.2f}",
                f"{trend_change:.2f}%"
            )
            
            self.result_tree.insert('', tk.END, values=values)
            
        self.log(f"扫描完成，找到 {len(results)} 只符合条件的股票")
        
    def scan_finished(self):
        self.scan_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)
        self.progress_var.set(0)
        self.status_var.set("扫描完成")
        self.is_scanning = False
        self.refresh_universe_var.set(False)
        
    def on_stock_select(self, event):
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item['values'][0]
            self.show_stock_detail(stock_code)
            
    def on_stock_double_click(self, event):
        selection = self.result_tree.selection()
        if selection:
            item = self.result_tree.item(selection[0])
            stock_code = item['values'][0]
            self.show_stock_detail(stock_code)
            self.notebook.select(1)
            
    def query_single_stock(self):
        stock_code = self.stock_code_var.get().strip()
        if not stock_code:
            messagebox.showwarning("警告", "请输入股票代码")
            return
        self.show_stock_detail(stock_code)
        self.notebook.select(1)
        
    def show_stock_detail(self, stock_code: str):
        self.log(f"查询股票 {stock_code} 详情...")
        self.status_var.set(f"正在查询 {stock_code}...")
        
        detail_thread = threading.Thread(target=self._load_detail, args=(stock_code,), daemon=True)
        detail_thread.start()
        
    def _load_detail(self, stock_code: str):
        try:
            detail = self.stock_filter.get_stock_detail(stock_code)
            self.root.after(0, lambda: self._update_detail_ui(detail))
        except Exception as e:
            self.root.after(0, lambda: self.log(f"查询详情出错: {e}"))
        finally:
            self.root.after(0, lambda: self.status_var.set("查询完成"))
            
    def _update_detail_ui(self, detail: Dict[str, Any]):
        realtime = detail.get('realtime') or {}
        flow = detail.get('flow') or {}
        inner_outer = detail.get('inner_outer') or {}
        
        self.detail_labels['code'].config(text=detail['code'])
        self.detail_labels['name'].config(text=realtime.get('name', '-'))
        self.detail_labels['price'].config(text=f"{realtime.get('price', 0):.2f}")
        self.detail_labels['change_pct'].config(text=f"{realtime.get('change_pct', 0):.2f}%")
        self.detail_labels['volume'].config(text=f"{realtime.get('volume', 0):.0f}")
        self.detail_labels['amount'].config(text=f"{realtime.get('amount', 0):.2f}")
        self.detail_labels['volume_ratio'].config(text=f"{realtime.get('volume_ratio', 0):.2f}")
        self.detail_labels['turnover_rate'].config(text=f"{realtime.get('turnover_rate', 0):.2f}%")
        
        self.detail_labels['main_net_inflow'].config(text=f"{flow.get('main_net_inflow', 0):.2f}万")
        self.detail_labels['main_net_inflow_pct'].config(text=f"{flow.get('main_net_inflow_pct', 0):.2f}%")
        self.detail_labels['retail_net_inflow'].config(text=f"{flow.get('retail_net_inflow', 0):.2f}万")
        self.detail_labels['retail_net_inflow_pct'].config(text=f"{flow.get('retail_net_inflow_pct', 0):.2f}%")
        
        self.detail_labels['inner_volume'].config(text=f"{inner_outer.get('inner_volume', 0):.0f}")
        self.detail_labels['outer_volume'].config(text=f"{inner_outer.get('outer_volume', 0):.0f}")
        self.detail_labels['inner_outer_ratio'].config(text=f"{inner_outer.get('inner_outer_ratio', 0):.2f}")
        self.detail_labels['outer_pct'].config(text=f"{inner_outer.get('outer_pct', 0):.2f}%")
        
        self._draw_chart(detail.get('history'))
        
    def _draw_chart(self, history):
        self.ax.clear()
        
        if history is None or history.empty:
            self.ax.text(0.5, 0.5, '暂无数据', ha='center', va='center', fontsize=14)
            self.canvas.draw()
            return
            
        dates = history['date'].values if 'date' in history.columns else range(len(history))
        closes = history['close'].values if 'close' in history.columns else []
        
        self.ax.plot(dates, closes, 'b-', linewidth=1.5, label='收盘价')
        
        if 'high' in history.columns and 'low' in history.columns:
            self.ax.fill_between(dates, history['low'].values, history['high'].values, alpha=0.3, label='波动区间')
        
        self.ax.set_xlabel('日期')
        self.ax.set_ylabel('价格')
        self.ax.set_title('近期走势')
        self.ax.legend()
        self.ax.grid(True, alpha=0.3)
        
        self.ax.tick_params(axis='x', rotation=45)
        
        self.fig.tight_layout()
        self.canvas.draw()
        
    def export_results(self):
        if not self.filtered_stocks:
            messagebox.showwarning("警告", "没有可导出的结果")
            return
            
        from tkinter import filedialog
        import csv
        
        file_path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV文件", "*.csv"), ("所有文件", "*.*")]
        )
        
        if file_path:
            try:
                with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
                    writer = csv.writer(f)
                    writer.writerow(['代码', '名称', '现价', '涨跌幅', '量比', '内外盘比', '主力净流入(万)', '连续涨幅%'])
                    
                    for result in self.filtered_stocks:
                        data = result.get('data', {})
                        realtime = data.get('realtime') or {}
                        flow = data.get('flow') or {}
                        inner_outer = data.get('inner_outer') or {}
                        history = data.get('history')
                        
                        trend_change = 0
                        nd = self.stock_filter.trend_days
                        if history is not None and len(history) >= nd:
                            recent = history.tail(nd)
                            if 'change_pct' in recent.columns:
                                trend_change = recent['change_pct'].sum()
                        
                        writer.writerow([
                            result['code'],
                            realtime.get('name', '-'),
                            f"{realtime.get('price', 0):.2f}",
                            f"{realtime.get('change_pct', 0):.2f}%",
                            f"{realtime.get('volume_ratio', 0):.2f}",
                            f"{inner_outer.get('inner_outer_ratio', 0):.2f}",
                            f"{flow.get('main_net_inflow', 0):.2f}",
                            f"{trend_change:.2f}%"
                        ])
                        
                messagebox.showinfo("成功", f"结果已导出到 {file_path}")
                self.log(f"结果已导出到 {file_path}")
            except Exception as e:
                messagebox.showerror("错误", f"导出失败: {e}")
                
    def show_settings(self):
        settings_window = tk.Toplevel(self.root)
        settings_window.title("筛选条件设置")
        settings_window.geometry("400x300")
        settings_window.transient(self.root)
        settings_window.grab_set()
        
        frame = ttk.Frame(settings_window, padding="20")
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="量比阈值:").grid(row=0, column=0, sticky=tk.E, pady=5)
        ttk.Entry(frame, textvariable=self.volume_ratio_var, width=15).grid(row=0, column=1, pady=5)
        
        ttk.Label(frame, text="内外盘比阈值:").grid(row=1, column=0, sticky=tk.E, pady=5)
        ttk.Entry(frame, textvariable=self.inner_outer_var, width=15).grid(row=1, column=1, pady=5)
        
        ttk.Label(frame, text="主力净流入(万):").grid(row=2, column=0, sticky=tk.E, pady=5)
        ttk.Entry(frame, textvariable=self.net_inflow_var, width=15).grid(row=2, column=1, pady=5)
        
        ttk.Label(frame, text="连续天数(收盘>MA5):").grid(row=3, column=0, sticky=tk.E, pady=5)
        ttk.Entry(frame, textvariable=self.trend_days_var, width=15).grid(row=3, column=1, pady=5)
        
        ttk.Label(frame, text="最低价格:").grid(row=4, column=0, sticky=tk.E, pady=5)
        ttk.Entry(frame, textvariable=self.min_price_var, width=15).grid(row=4, column=1, pady=5)
        
        ttk.Label(frame, text="最高价格:").grid(row=5, column=0, sticky=tk.E, pady=5)
        ttk.Entry(frame, textvariable=self.max_price_var, width=15).grid(row=5, column=1, pady=5)
        
        ttk.Button(frame, text="保存", command=lambda: [self.update_filter_params(), settings_window.destroy()]).grid(row=6, column=0, columnspan=2, pady=20)
        
    def show_about(self):
        messagebox.showinfo("关于", "股票监控程序 v1.0\n\n功能:\n- 根据量比、内外盘比等筛选股票\n- 连续N日收盘站上MA5（默认5日）\n- 可视化K线图\n\n数据来源: akshare")
