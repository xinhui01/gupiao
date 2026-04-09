"""
网络相关：SSL 校验见 USE_INSECURE_SSL / GUPPIAO_INSECURE_SSL；
代理报错见 USE_BYPASS_PROXY / GUPPIAO_BYPASS_PROXY（见 README）。
"""
import os
import platform
import sys
import tkinter as tk

# 单开关：True=不走系统代理（推荐，避免代理导致东财接口断连）；False=沿用系统代理
BYPASS_PROXY = True


def _check_runtime() -> None:
    is_macos = platform.system() == "Darwin"
    is_pyenv = ".pyenv" in (sys.executable or "")
    is_py313 = sys.version_info[:2] >= (3, 13)
    if is_macos and is_pyenv and is_py313:
        raise SystemExit(
            "当前运行环境是 macOS + pyenv Python 3.13，这个组合在 Tk GUI 下容易直接 bus error/abort。\n"
            "建议改用 Python 3.11/3.12 重新创建虚拟环境后再启动。"
        )


def main():
    _check_runtime()
    if BYPASS_PROXY:
        os.environ["GUPPIAO_BYPASS_PROXY"] = "1"
    else:
        os.environ.pop("GUPPIAO_BYPASS_PROXY", None)

    from stock_logger import get_logger
    logger = get_logger(__name__)
    logger.info("应用启动")

    from stock_gui import StockMonitorApp
    from stock_store import ensure_store_ready

    ensure_store_ready()
    root = tk.Tk()
    app = StockMonitorApp(root)
    logger.info("主窗口已初始化，进入主循环")
    root.mainloop()
    logger.info("应用退出")


if __name__ == "__main__":
    main()
