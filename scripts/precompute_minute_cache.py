"""
分足バックテスト用の事前キャッシュ生成（高速版）

月データを1回ロードし、(Date, Code4)でグルーピングして一括抽出。

Usage:
    python scripts/precompute_minute_cache.py

生成物:
    data/cache/minute_bars_cache.pkl
"""
import os
import sys
import glob
import pickle
import time
import pandas as pd
from datetime import datetime
from collections import defaultdict
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level:<7} | {message}")


def normalize_code(code_str):
    try:
        code_int = int(code_str)
        return str(code_int // 10) if code_int >= 10000 else code_str
    except ValueError:
        return code_str


def main():
    logger.info("=" * 60)
    logger.info("分足キャッシュ事前生成（高速版）")
    logger.info("=" * 60)

    # 1. 全candidates CSVから必要な(trade_date, code)ペアを収集
    files = sorted(glob.glob("data/candidates_*.csv"))
    files = [f for f in files if 'test' not in f.lower()]
    logger.info(f"候補CSV: {len(files)}ファイル")

    # month -> set of (date_str_yyyy-mm-dd, code_4digit)
    needed_by_month = defaultdict(set)

    for file_path in files:
        filename = os.path.basename(file_path)
        date_str = filename.replace("candidates_", "").replace(".csv", "")
        try:
            trade_date = datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            continue

        month_key = (trade_date.year, trade_date.month)
        date_iso = trade_date.strftime('%Y-%m-%d')

        df = pd.read_csv(file_path, encoding='utf-8-sig')
        for code_raw in df['Code'].astype(str).unique():
            code4 = normalize_code(code_raw)
            needed_by_month[month_key].add((date_str, date_iso, code4))

    total_pairs = sum(len(v) for v in needed_by_month.values())
    logger.info(f"必要ペア数: {total_pairs}件, 対象月: {len(needed_by_month)}ヶ月")

    # 2. 月ごとにgzipを1回ロードし、必要なデータだけ抽出
    cache = {}
    t0 = time.time()

    # gzipファイルの場所マッピング
    gz_files = {}
    for gz in glob.glob("equities_bars_minute/**/*.csv.gz", recursive=True):
        # equities_bars_minute_202504.csv.gz -> (2025, 4)
        base = os.path.basename(gz)
        ym = base.replace("equities_bars_minute_", "").replace(".csv.gz", "")
        try:
            year = int(ym[:4])
            month = int(ym[4:6])
            gz_files[(year, month)] = gz
        except ValueError:
            continue

    for month_key in sorted(needed_by_month.keys()):
        gz_path = gz_files.get(month_key)
        if gz_path is None:
            # 別ディレクトリにある場合（2025フォルダに202601があるケース等）
            for mk, gp in gz_files.items():
                if mk == month_key:
                    gz_path = gp
                    break
        if gz_path is None:
            logger.warning(f"{month_key}: gzipファイルなし")
            continue

        logger.info(f"{month_key[0]}-{month_key[1]:02d}: {gz_path} ロード中...")
        t1 = time.time()

        df_month = pd.read_csv(gz_path, dtype={'Code': str})
        df_month['Code4'] = df_month['Code'].apply(normalize_code)

        t2 = time.time()
        logger.info(f"  ロード完了: {len(df_month):,}行 ({t2-t1:.1f}秒)")

        # Date+Code4でグルーピング（1回のgroupbyで全ペアを抽出）
        grouped = df_month.groupby(['Date', 'Code4'])

        pairs = needed_by_month[month_key]
        found = 0
        for date_str, date_iso, code4 in pairs:
            try:
                bars = grouped.get_group((date_iso, code4)).sort_values('Time').reset_index(drop=True)
                cache[(date_str, code4)] = bars
                found += 1
            except KeyError:
                pass

        t3 = time.time()
        logger.info(f"  抽出完了: {found}/{len(pairs)}件 ({t3-t2:.1f}秒)")

        del df_month  # メモリ解放

    elapsed = time.time() - t0
    logger.info(f"全抽出完了: {len(cache)}件 ({elapsed:.0f}秒)")

    # 3. pickle保存
    cache_path = "data/cache/minute_bars_cache.pkl"
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    logger.info(f"保存中: {cache_path}")
    with open(cache_path, 'wb') as f:
        pickle.dump(cache, f)

    size_mb = os.path.getsize(cache_path) / 1024 / 1024
    logger.info(f"保存完了: {size_mb:.0f}MB, {len(cache)}エントリ")


if __name__ == "__main__":
    main()
