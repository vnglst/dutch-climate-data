import json
import requests
import datetime

URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/maandgegevens/mndgeg_260_tg.txt'


def fetch_data(url):
    return requests.get(url, timeout=10).text


def parse_data(raw_data):
    lines = [l for l in raw_data.split('\n')[15:] if l.strip()]
    return [parse_line(l) for l in lines]


def parse_line(l):
    cols = l.strip().replace('\r', '').split(',')
    year = cols[1]
    vals = [year]
    for v in cols[2:]:
        if v.strip():
            vals.append(float(v) / 10)
        else:
            vals.append(None)
    return vals


def pivot_table(parsed_data):
    monthly_data = [[] for _ in range(13)]
    for row in parsed_data:
        for month, value in enumerate(row[1:]):
            if value is not None:
                monthly_data[month].append(value)
    return monthly_data


def calculate_avg_temps(monthly_data):
    return [sum(vals[:100]) / len(vals[:100]) for vals in monthly_data]


def calculate_anomalies(parsed_data, avg_temps):
    anomalies = []
    for row in parsed_data:
        year = row[0]
        months = row[1:-1]
        for month_index, value in enumerate(months):
            avg = avg_temps[month_index]
            anomaly = None if value is None else value - avg
            anomalies.append([month_index, year, anomaly])
    return anomalies


def write_to_file(filename, data, years):
    to_write = {
        'timestamp': datetime.datetime.now().isoformat(),
        'data': data,
        'years': years
    }
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(to_write, f, indent=2)


def calculate_yearly_anomalies(parsed_data, avg_temps):
    yearly_anomalies = []
    for row in parsed_data:
        year_value = row[-1]
        if year_value is not None:
            anomaly = year_value - avg_temps[-1]
        else:
            anomaly = None
        yearly_anomalies.append(anomaly)
    return yearly_anomalies


def main():
    raw_data = fetch_data(URL)
    parsed_data = parse_data(raw_data)
    monthly_data = pivot_table(parsed_data)
    avg_temps = calculate_avg_temps(monthly_data)
    anomalies = calculate_anomalies(parsed_data, avg_temps)
    years = [row[0] for row in parsed_data]
    write_to_file('data/temperature-heatmap.json',
                  anomalies[::-1], years[::-1])
    yearly_anomalies = calculate_yearly_anomalies(parsed_data, avg_temps)
    write_to_file('data/temperature-anomalies.json',
                  yearly_anomalies, years)


if __name__ == "__main__":
    main()
