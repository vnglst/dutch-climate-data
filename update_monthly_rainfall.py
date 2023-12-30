import pandas as pd
import io
import zipfile
import requests
import json
import datetime

URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/daggegevens/etmgeg_260.zip'
MONTHLY_OUTPUT_FILE = 'data/rainfall-heatmap.json'


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
    df['TG'] = pd.to_numeric(df['TG'], errors='coerce')

    # set any -1 values to 0, in the data -1 is used for <0.05 mm
    df['RH'] = df['RH'].apply(lambda x: 0 if x == -1 else x)
    # divide by 10 to get mm
    df['RH'] = df['RH'] / 10
    # divide by 10 to get degrees celcius
    df['TG'] = df['TG'] / 10
    df = df[['YYYYMMDD', 'RH', 'TG']]
    return df


def remove_empty_rows(df):
    mask = (df['YYYYMMDD'].dt.month == 1) & (
        df['YYYYMMDD'].dt.day == 1) & (df['RH'].notna())
    first_valid_index = mask.idxmax()
    df = df.loc[first_valid_index:]
    return df


def sum_by_month(df):
    df = df.groupby(df['YYYYMMDD'].dt.strftime('%Y%m'))[
        ['RH', 'TG']].mean().reset_index()
    df = df.rename(columns={'YYYYMMDD': 'YYYYMM'})
    df['YYYYMM'] = pd.to_datetime(df['YYYYMM'], format='%Y%m')
    return df


def calculcate_monthly_rainfall(df, output_file):
    df = normalize(df)
    df = remove_empty_rows(df)
    df = sum_by_month(df)

    years = df['YYYYMM'].dt.year.tolist()
    years = [str(year) for year in set(years)]

    rainfall_heatmap = []
    temperature_heatmap = []

    for index, row in df.iterrows():
        month_nr = row['YYYYMM'].month - 1
        year_str = str(row['YYYYMM'].year)
        rainfall = row['RH'] if not pd.isna(row['RH']) else None
        temperatures = row['TG'] if not pd.isna(row['TG']) else None
        rainfall_heatmap.append([year_str, month_nr, rainfall])
        temperature_heatmap.append([year_str, month_nr, temperatures])

    return {
        'rainfall_heatmap': rainfall_heatmap,
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

    data = calculcate_monthly_rainfall(df, output_file)
    write_to_file(output_file, data)


if __name__ == "__main__":
    main()
