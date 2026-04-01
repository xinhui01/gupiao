"""
网络相关：SSL 校验见 USE_INSECURE_SSL / GUPPIAO_INSECURE_SSL；
代理报错见 USE_BYPASS_PROXY / GUPPIAO_BYPASS_PROXY（见 README）。
"""
import os
import tkinter as tk
from stock_gui import StockMonitorApp
from stock_store import ensure_store_ready

# 单开关：True=不走系统代理（推荐，避免代理导致东财接口断连）；False=沿用系统代理
BYPASS_PROXY = True


def main():
    if BYPASS_PROXY:
        os.environ["GUPPIAO_BYPASS_PROXY"] = "1"
    else:
        os.environ.pop("GUPPIAO_BYPASS_PROXY", None)
    ensure_store_ready()
    root = tk.Tk()
    app = StockMonitorApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
