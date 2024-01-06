import pandas as pd
import io
import zipfile
import requests
import json
import datetime

URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_260.zip'
OUTPUT_FILE = 'data/yearly-weather-data.json'


def download_and_unzip(url):
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        for filename in z.namelist():
            with z.open(filename) as f:
                return f.read().decode('utf-8')


def write_to_file(filename, data):
    to_write = {
        'timestamp': datetime.datetime.now().isoformat(),
    }

    to_write.update(data)

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(to_write, f, indent=2, allow_nan=False)


def normalize(df):
    df['YYYYMMDD'] = pd.to_datetime(df['YYYYMMDD'], format='%Y%m%d')
    df['RH'] = pd.to_numeric(df['RH'], errors='coerce')
    df['SQ'] = pd.to_numeric(df['SQ'], errors='coerce')
    df['TG'] = pd.to_numeric(df['TG'], errors='coerce')

    # Replace -1 values with 0
    df[['RH', 'SQ']] = df[['RH', 'SQ']].replace(-1, 0)

    # Divide by 10 to get mm/hours/Celsius
    df[['RH', 'SQ', 'TG']] = df[['RH', 'SQ', 'TG']].div(10)

    df = df[['YYYYMMDD', 'RH', 'SQ', 'TG']]
    return df


def remove_empty_rows(df):
    mask = (df['YYYYMMDD'].dt.month == 1) & (
        df['YYYYMMDD'].dt.day == 1) & (df['RH'].notna())
    first_valid_index = mask.idxmax()
    last_valid_index = mask[::-1].idxmax() - 1
    df = df.loc[first_valid_index:last_valid_index]
    return df


def group_by_year(df):
    df = df.groupby(df['YYYYMMDD'].dt.year).agg(
        {'RH': 'sum', 'SQ': 'sum', 'TG': 'mean'}).reset_index()
    df = df.rename(columns={'YYYYMMDD': 'YYYY'})
    return df


def calc_anomalies(df):
    for column in ['RH', 'SQ', 'TG']:
        mean_until_2000 = df[df['YYYY'] <= 2000][column].mean()
        df[f'{column.lower()}_mean'] = mean_until_2000
        df[f'{column.lower()}_anomaly'] = df[column] - mean_until_2000
    return df


def anomaly_trend(df):
    for column in ['rh_anomaly', 'sq_anomaly', 'tg_anomaly']:
        df[f'{column}_trend'] = df[column].rolling(10, min_periods=1).mean()
    return df


def process_anomalies(df):
    df = normalize(df)
    df = remove_empty_rows(df)
    df = group_by_year(df)
    df = calc_anomalies(df)
    df = anomaly_trend(df)

    years = df['YYYY'].tolist()
    years = [str(year) for year in years]
    last_year = int(years[-1])

    return {
        'mean_rainfall': df['rh_mean'].iloc[-1],
        'rainfall_anomalies': df['rh_anomaly'].tolist(),
        'rainfall_trend': df['rh_anomaly_trend'].tolist(),
        'mean_sunshine': df['sq_mean'].iloc[-1],
        'sunshine_anomalies': df['sq_anomaly'].tolist(),
        'sunshine_trend': df['sq_anomaly_trend'].tolist(),
        'mean_temperature': df['tg_mean'].iloc[-1],
        'temperature_anomalies': df['tg_anomaly'].tolist(),
        'temperature_trend': df['tg_anomaly_trend'].tolist(),
        # Source: https://www.knmi.nl/klimaatdashboard
        'max_temperature_anomaly': df['tg_anomaly'].max(),
        'min_temperature_anomaly': df['tg_anomaly'].min(),
        'temperature_worst_case': 14.9 - df['tg_mean'].iloc[-1],
        'temperature_best_case': 11.4 - df['tg_mean'].iloc[-1],
        'last_temperature_anomaly': df['tg_anomaly'].iloc[-1],
        'last_temperature_trend': df['tg_anomaly_trend'].iloc[-1],
        'years': years,
        'forecast_years': years + [str(year) for year in range(last_year+1, 2101)]
    }


def main(url=URL, output_file=OUTPUT_FILE):
    content = download_and_unzip(url)

    skip_rows = 0
    for line in content.split('\n'):
        if line.startswith('# STN'):
            break
        skip_rows += 1

    df = pd.read_csv(io.StringIO(content),
                     skiprows=skip_rows, low_memory=False)

    df.columns = df.columns.str.strip()

    data = process_anomalies(df)
    write_to_file(output_file, data)


if __name__ == "__main__":
    main()
