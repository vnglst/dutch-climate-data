import pandas as pd
import io
import zipfile
import requests
import json
import datetime

URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_260.zip'
OUTPUT_FILE = 'data/rainfall.json'


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
    df['RH'] = df['RH'].str.strip()
    df['RH'] = pd.to_numeric(df['RH'], errors='coerce')

    # set any -1 values to 0, in the data -1 is used for <0.05 mm
    df['RH'] = df['RH'].apply(lambda x: 0 if x == -1 else x)
    # divide by 10 to get mm
    df['RH'] = df['RH'] / 10
    df = df[['YYYYMMDD', 'RH']]
    return df


def remove_empty_rows(df):
    mask = (df['YYYYMMDD'].dt.month == 1) & (
        df['YYYYMMDD'].dt.day == 1) & (df['RH'].notna())
    first_valid_index = mask.idxmax()
    df = df.loc[first_valid_index:]
    return df


def sum_by_year(df):
    df = df.groupby(df['YYYYMMDD'].dt.year)['RH'].sum().reset_index()
    df = df.rename(columns={'YYYYMMDD': 'YYYY'})
    return df


def calc_anomalies(df):
    mean_until_2000 = df[df['YYYY'] <= 2000]['RH'].mean()
    df['anomaly'] = df['RH'] - mean_until_2000
    return df


def anomaly_trend(df):
    df['anomaly_trend'] = df['anomaly'].rolling(10, min_periods=1).mean()
    return df


def calculate_yearly_rainfall(df):
    df = normalize(df)
    df = remove_empty_rows(df)
    df = sum_by_year(df)
    df = calc_anomalies(df)
    df = anomaly_trend(df)

    years = df['YYYY'].tolist()
    years = [str(year) for year in years]

    return {
        'mean': df['RH'].mean(),
        'anomalies': df['anomaly'].tolist(),
        'trend': df['anomaly_trend'].tolist(),
        'years': years,
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

    data = calculate_yearly_rainfall(df)
    write_to_file(output_file, data)


if __name__ == "__main__":
    main()
