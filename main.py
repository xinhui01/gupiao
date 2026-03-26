"""
网络相关：SSL 校验见 USE_INSECURE_SSL / GUPPIAO_INSECURE_SSL；
代理报错见 USE_BYPASS_PROXY / GUPPIAO_BYPASS_PROXY（见 README）。
"""
import tkinter as tk
from stock_gui import StockMonitorApp
from stock_store import ensure_store_ready


def main():
    ensure_store_ready()
    root = tk.Tk()
    app = StockMonitorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
