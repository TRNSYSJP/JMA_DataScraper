# -*- coding: utf-8 -*-
import os
import datetime
import csv
import urllib.request
from bs4 import BeautifulSoup
import sys
import time
import argparse
from datetime import timedelta
from tqdm import tqdm
from urllib.error import URLError
from typing import List, Optional

# 定数定義
REQUEST_INTERVAL = 1  # サーバーへのリクエスト間隔（秒）
MISSING_VALUE = -9999  # 欠測値
MAX_RETRIES = 3  # URLリトライ回数
BACKOFF_FACTOR = 0.5  # エクスポネンシャルバックオフの係数

# 風向→角度の変換テーブル、ただし'静穏'は0に変換
WIND_DIRECTION = {  
    '北': 0, '北北東': 22.5, '北東': 45, '東北東': 67.5,
    '東': 90, '東南東': 112.5, '南東': 135, '南南東': 157.5,
    '南': 180, '南南西': 202.5, '南西': 225, '西南西': 247.5,
    '西': 270, '西北西': 292.5, '北西': 315, '北北西': 337.5,
    '静穏': 0,
}

def str2windir(wind_str: Optional[str]) -> float:
    """
    風向を角度に変換する関数
    '静穏'は0に変換、null, '#', '×'は-9999を返す
    
    Args:
        wind_str: 変換対象の風向文字列
     
    Returns:
        変換後の角度、変換できない場合は-9999
    """
    try:
        # null, '#', '×'は欠測値
        if wind_str is None or wind_str in ('#', '×'):
            return MISSING_VALUE
        
        # 余分な文字を削除
        wind_str = wind_str.replace(')', '').strip()
        
        # テーブルから取得
        return WIND_DIRECTION[wind_str]
    
    except (KeyError, AttributeError):
        # 上記以外は数値に変換できない欠測地と見做す
        # ///, ], etc        
        return MISSING_VALUE
    

def str2float(weather_data: Optional[str]) -> float:
    """
    文字列を浮動小数点数に変換する関数
    数値に変換できない場合は-9999を返す（欠測値は-9999とする）

    Args:
        weather_data: 変換対象の文字列

    Returns:
        変換後の浮動小数点数、変換できない場合は-9999
    """
    try:
        # null(空白、空欄)は0と見做す
        if weather_data is None:
            return 0.0
        
        # weather_data が文字列だった場合、余分な文字を削除
        if isinstance(weather_data, str):
            # 準正常値（風速に含まれる' )'を削除 & 空白を削除）
            weather_data = weather_data.replace(')', '').strip()
        
        # 0と見做す記号の処理
        if weather_data == "--":
            return 0.0
        
        return float(weather_data)
        
    except (ValueError, TypeError):
        # 数値に変換できない場合は欠測地と見做す
        # ×, ///, #, etc
        return MISSING_VALUE

def mj2w(mj_value: float) -> float:
    """
    全天日射量をMJ/m2からW/m2に変換する関数
    
    Args:
        mj_value: MJ/m2単位の日射量
    
    Returns:
        W/m2単位の日射量
    """
    if mj_value >= 0:
        return mj_value * 10**6 / 3600  # 1時間 = 3600秒
    else:
        return 0.0
  
def fetch_url_with_retry(url: str, max_retries: int = MAX_RETRIES, 
                        backoff_factor: float = BACKOFF_FACTOR):
    """
    指定されたURLからHTMLを取得する関数（リトライ機能付き）
    
    Args:
        url: 取得対象のURL
        max_retries: リトライ回数
        backoff_factor: エクスポネンシャルバックオフの係数
    
    Returns:
        取得したHTMLレスポンス

    Raises:
        Exception: 最大リトライ回数に達した後もリクエストが成功しない場合
    """
    for i in range(max_retries):
        try:
            response = urllib.request.urlopen(url)
            return response
        except URLError as e:
            print(f"リクエストエラー: {e.reason}. リトライ {i + 1}/{max_retries}")
            if i < max_retries - 1:  # 最後のリトライでない場合のみ待機
                time.sleep(backoff_factor * (2 ** i))  # エクスポネンシャルバックオフ

    raise Exception(f"{max_retries} 回のリトライ後もURLの取得に失敗しました。")

def scraping(url: str, date: datetime.date) -> List[List]:
    """
    気象データをスクレイピングする関数
    
    Args:
        url: スクレイピング対象のURL
        date: 対象日付
    
    Returns:
        1日分の時間ごとの気象データリスト
    """
    try:
        # 気象データのページを取得
        response = fetch_url_with_retry(url)
        html = response.read()

        # HTTPステータスコードのチェック
        if response.getcode() != 200:
            print(f"Error: HTTP status code is {response.getcode()}")
            sys.exit(1)

        # HTMLの基本的な構造のチェック
        soup = BeautifulSoup(html, 'html.parser')
        if not soup.html or not soup.head or not soup.body:
            print("Error: HTML structure is not complete")
            sys.exit(1)
            
    except URLError as e:
        print(f"URL Error: {e.reason}")
        sys.exit(1)
        
    except Exception as e:
        print(f"Error: {e}")
        print('HTMLが正常に取得できませんでした。再実行してください')
        sys.exit(1)
    
    # テーブルデータを解析
    soup = BeautifulSoup(html, features="html.parser")
    table = soup.find("table", {"class": "data2_s"})
    
    if not table:
        print(f"Error: テーブルが見つかりませんでした。日付: {date}")
        sys.exit(1)

    data_list_per_hour = []

    # table の中身を取得（最初の2行はヘッダーなのでスキップ）
    for tr in table.find_all('tr')[2:]:
        tds = tr.find_all('td')
        
        if len(tds) < 14:
            continue  # データが不完全な行はスキップ

        # 項目別に値を処理する
        data_row = [
            date,                                      # 年月日
            tds[0].string,                             # 時間
            str2float(tds[1].string),                  # 気圧現地(hPa)
            str2float(tds[2].string),                  # 気圧海面(hPa)
            str2float(tds[3].string),                  # 降水量(mm)
            str2float(tds[4].string),                  # 気温(℃)
            str2float(tds[5].string),                  # 露点温度(℃)
            str2float(tds[6].string),                  # 蒸気圧(hPa)
            str2float(tds[7].string),                  # 湿度(%)
            str2float(tds[8].string),                  # 風速(m/s)
            str2windir(tds[9].string),                 # 風向→角度変換
            str2float(tds[10].string),                 # 日照時間(h)
            mj2w(str2float(tds[11].string)),           # 全天日射 MJ/m2 → W/m2
            str2float(tds[12].string),                 # 降雪(cm)
            str2float(tds[13].string),                 # 積雪(cm)
        ]
        
        data_list_per_hour.append(data_row)

    return data_list_per_hour

def create_csv(
    prec_no: int,
    block_no: int,
    start_date: datetime.date,
    end_date: datetime.date,
    request_interval: float = REQUEST_INTERVAL,
):
    """
    気象データをCSVファイルとして保存する関数
    
    Args:
        prec_no: 観測所番号
        block_no: ブロック番号
        start_date: データ取得開始日
        end_date: データ取得終了日
        request_interval: サーバーへのリクエスト間隔（秒）
    """
    # CSV 出力先ディレクトリ
    output_dir = r".\weather"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # 出力ファイル名
    start_year = str(start_date.year)
    end_year = str(end_date.year)
    output_file = f"{prec_no}_{block_no}_{start_year}_{end_year}_weather.csv"

    # CSV の列（天気、雲量、視程は今回は対象外とする）
    fields = [
        "年月日", "時間", "気圧現地(hPa)", "気圧海面(hPa)",
        "降水量(mm)", "気温(℃)", "露点温度(℃)", "蒸気圧(hPa)", "湿度(%)",
        "風速(m/s)", "風向(deg)", "日照時間(h)", "全天日射量(W/m2)", "降雪(cm)", "積雪(cm)"
    ]

    total_days = (end_date - start_date).days + 1  # total number of days to process
    sw_start = time.time()  # start time
    
    # CSVファイルへの書き込み
    output_path = os.path.join(output_dir, output_file)
    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, lineterminator='\r\n')
        writer.writerow(fields)

        current_date = start_date
        # tqdmのプログレスバーに経過時間と残り時間を表示
        progress_bar = tqdm(
            range(total_days), 
            desc="Processing", 
            unit="day",
            bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [Elapsed: {elapsed}, Remaining: {remaining}, {rate_fmt}]'
        )
        
        for _ in progress_bar:
            # 対象URL（今回は東京）
            url = (
                f"http://www.data.jma.go.jp/obd/stats/etrn/view/hourly_s1.php?"
                f"prec_no={prec_no}&block_no={block_no}"
                f"&year={current_date.year}&month={current_date.month}"
                f"&day={current_date.day}&view="
            )

            # 日単位、1時間ごとの気象データを取得
            data_per_day = scraping(url, current_date)

            # 1日分のデータをCSVに書き込み
            for row in data_per_day:
                writer.writerow(row)
                
            # サーバに負担を掛けないようにインターバルを入れる
            time.sleep(request_interval)
            
            current_date += datetime.timedelta(days=1)
 
    # sw_timeからの経過時間を表示
    sw_time = time.time() - sw_start
    print()
    print("=" * 60)
    print(f"Processing completed successfully!")
    print(f"Total elapsed time: {str(timedelta(seconds=int(sw_time)))}")
    print("=" * 60)
    print(f"Data saved to: {output_path}")


def parse_arguments():
    """
    コマンドライン引数を解析する関数

    Returns:
        解析済みの引数オブジェクト
    """
    parser = argparse.ArgumentParser(
        description="気象庁のアメダスデータをスクレイピングしてCSVファイルに保存します（プログレスバー表示版）。",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用例:
  # 東京（prec_no=44, block_no=47662）の2000年1月のデータを取得
  python get_amedas_progressbar.py --prec_no 44 --block_no 47662 --start 2000-01-01 --end 2000-01-31

  # 大阪（prec_no=62, block_no=47772）の2020年全体のデータを取得
  python get_amedas_progressbar.py --prec_no 62 --block_no 47772 --start 2020-01-01 --end 2020-12-31
        """,
    )

    parser.add_argument(
        "--prec_no", type=int, required=True, help="観測所番号（例: 東京=44, 大阪=62）"
    )

    parser.add_argument(
        "--block_no",
        type=int,
        required=True,
        help="ブロック番号（例: 東京=47662, 大阪=47772）",
    )

    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="データ取得開始日（YYYY-MM-DD形式、例: 2000-01-01）",
    )

    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="データ取得終了日（YYYY-MM-DD形式、例: 2000-01-31）",
    )

    parser.add_argument(
        "--interval",
        type=float,
        default=REQUEST_INTERVAL,
        help=f"サーバーへのリクエスト間隔（秒）。デフォルト: {REQUEST_INTERVAL}秒",
    )

    return parser.parse_args()


if __name__ == '__main__':
    # コマンドライン引数を解析
    args = parse_arguments()

    # 日付文字列をdateオブジェクトに変換
    try:
        start_date = datetime.datetime.strptime(args.start, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(args.end, "%Y-%m-%d").date()
    except ValueError as e:
        print(
            f"エラー: 日付の形式が正しくありません。YYYY-MM-DD形式で指定してください。"
        )
        print(f"詳細: {e}")
        sys.exit(1)

    # 日付の妥当性チェック
    if start_date > end_date:
        print("エラー: 開始日が終了日より後になっています。")
        sys.exit(1)

    # パラメータを表示
    print("=" * 60)
    print("気象データ取得開始")
    print("=" * 60)
    print(f"観測所番号: {args.prec_no}")
    print(f"ブロック番号: {args.block_no}")
    print(f"取得期間: {start_date} ～ {end_date}")
    print(f"リクエスト間隔: {args.interval}秒")
    print("=" * 60)
    print()

    # CSVファイル作成
    create_csv(args.prec_no, args.block_no, start_date, end_date, args.interval)