import json
import requests
import datetime

URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/maandgegevens/mndgeg_260_tg.txt'


def parse_data(raw_data):
    lines = [line for line in raw_data.split('\n')[15:] if line.strip()]
    return [parse_line(line) for line in lines]


def parse_line(line):
    columns = line.strip().replace('\r', '').split(',')
    year = columns[1]
    values = [year]
    for str in columns[2:]:
        if str.strip():
            values.append(float(str) / 10)
        else:
            values.append(None)
    return values


def pivot_table(parsed_data):
    pivoted = [[] for _ in range(13)]
    for row in parsed_data:
        for month, value in enumerate(row[1:]):
            if value is not None:
                pivoted[month].append(value)
    return pivoted


def calc_avg_temps(monthly_data):
    return [sum(vals[:100]) / len(vals[:100]) for vals in monthly_data]


def calc_monthly_anomalies(parsed_data, avg_temps):
    anomalies = []
    for row in parsed_data:
        year_label = row[0]
        # skip the first column (year label) and last column (year total)
        periods = row[1:-1]
        for period_index, value in enumerate(periods):
            avg = avg_temps[period_index]
            anomaly = None if value is None else value - avg
            anomalies.append([period_index, year_label, anomaly])
    return anomalies


def calc_yearly_anomalies(parsed_data, avg_temps):
    yearly_anomalies = []
    for row in parsed_data:
        year_value = row[-1]
        if year_value is not None:
            anomaly = year_value - avg_temps[-1]
        else:
            anomaly = None
        yearly_anomalies.append(anomaly)
    return yearly_anomalies


def write_to_file(filename, data, years):
    to_write = {
        'timestamp': datetime.datetime.now().isoformat(),
        'data': data,
        'years': years
    }

    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(to_write, f, indent=2)


def main():
    raw_data = requests.get(URL, timeout=10).text
    parsed_data = parse_data(raw_data)
    pivoted = pivot_table(parsed_data)
    avg_temps = calc_avg_temps(pivoted)
    year_labels = [row[0] for row in parsed_data]

    anomalies = calc_monthly_anomalies(parsed_data, avg_temps)
    write_to_file('data/temperature-heatmap.json',
                  anomalies[::-1], year_labels[::-1])

    yearly_anomalies = calc_yearly_anomalies(parsed_data, avg_temps)
    write_to_file('data/temperature-anomalies.json',
                  yearly_anomalies, year_labels)


if __name__ == "__main__":
    main()
