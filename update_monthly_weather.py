import pandas as pd
import io
import zipfile
import requests
import json
import datetime

URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_260.zip'
MONTHLY_OUTPUT_FILE = 'data/monthly-weather-data.json'


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
    df = df.loc[first_valid_index:]
    return df


def group_by_month(df):
    df = df.groupby(df['YYYYMMDD'].dt.strftime('%Y%m')).agg(
        {'TG': 'mean', 'RH': 'sum', 'SQ': 'sum'}).reset_index()
    df = df.rename(columns={'YYYYMMDD': 'YYYYMM'})
    df['YYYYMM'] = pd.to_datetime(df['YYYYMM'], format='%Y%m')
    return df


def calc_anomalies(df, key):
    df['Date'] = pd.to_datetime(df['YYYYMM'], format='%Y%m')
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month

    monthly_totals = {}
    for index, row in df.iterrows():
        month = row['Month']
        year = row['Year']

        if year >= 2000:
            break

        if month not in monthly_totals:
            monthly_totals[month] = []

        monthly_totals[month].append(row[key])

    monthly_averages = {}
    for month, values in monthly_totals.items():
        monthly_averages[month] = sum(values) / len(values)

    df[key + '_anomaly'] = df.apply(lambda row: row[key] -
                                    monthly_averages[row['Month']], axis=1)

    return df


def process_monthly_anomalies(df, output_file):
    df = normalize(df)
    df = remove_empty_rows(df)
    df = group_by_month(df)
    df = calc_anomalies(df, 'TG')
    df = calc_anomalies(df, 'RH')
    df = calc_anomalies(df, 'SQ')

    years = df['YYYYMM'].dt.year.tolist()
    years = [str(year) for year in set(years)]

    rainfall_heatmap = []
    sunshine_heatmap = []
    temperature_heatmap = []

    for index, row in df.iterrows():
        month_nr = row['YYYYMM'].month - 1
        year_str = str(row['YYYYMM'].year)
        rainfall_heatmap.append([year_str, month_nr, row['RH_anomaly']])
        sunshine_heatmap.append([year_str, month_nr, row['SQ_anomaly']])
        temperature_heatmap.append([year_str, month_nr, row['TG_anomaly']])

    return {
        'rainfall_heatmap': rainfall_heatmap,
        'sunshine_heatmap': sunshine_heatmap,
        'temperature_heatmap': temperature_heatmap,
        'years': years,
    }


def main(url=URL, output_file=MONTHLY_OUTPUT_FILE):
    content = download_and_unzip(url)

    skip_rows = 0
    for line in content.split('\n'):
        if line.startswith('# STN'):
            break
        skip_rows += 1

    df = pd.read_csv(io.StringIO(content),
                     skiprows=skip_rows, low_memory=False)

    df.columns = df.columns.str.strip()

    data = process_monthly_anomalies(df, output_file)
    write_to_file(output_file, data)


if __name__ == "__main__":
    main()
