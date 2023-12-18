import json
import requests

DATA_URL = 'https://cdn.knmi.nl/knmi/map/page/klimatologie/gegevens/maandgegevens/mndgeg_260_tg.txt'
r = requests.get(DATA_URL, timeout=10)
data = r.text

# parse data by splitting it across new lines, discard the first 15 lines
lines = data.split('\n')[15:]

# remove empty lines
lines = [line for line in lines if line != '']

parsed = []

for year, line in enumerate(lines):
    values = []
    columns = line.split(',')
    yearLabel = columns[1]
    values.append(yearLabel)
    str_values = columns[2:]

    for str_value in str_values:
        str_value = str_value.strip()
        str_value = str_value.replace('\r', '')

        if str_value == '':
            values.append(None)
        else:
            values.append(float(str_value) / 10)

    parsed.append(values)

# start with a list of 13 empty lists, one for each month and one for the year
pivoted = [[] for _ in range(13)]

for year, row in enumerate(parsed):
    monthly_values = []

    for month, value in enumerate(row[1:]):
        monthly_values.append(value)

        if value is not None:
            pivoted[month].append(value)


averages_last_century = []

for month, values in enumerate(pivoted):
    # only take the first 100 years
    values = values[:100]
    average = sum(values) / len(values)
    averages_last_century.append(average)

heatmap_data = []
heatmap_years = []

for year, line in enumerate(parsed):
    year_label = line[0]
    heatmap_years.append(year_label)
    months = line[1:-1]

    for month, value in enumerate(months):
        average = averages_last_century[month]
        anomaly = None if value is None else value - average
        heatmap_data.append([month, year_label, anomaly])

# reverse the list so that the most recent data is at the end
heatmap_data = heatmap_data[::-1]
heatmap_years = heatmap_years[::-1]

to_write = {
    'timestamp': datetime.datetime.now().isoformat(),
    'data': heatmap_data,
    'years': heatmap_years
}

with open('data/temperature-heatmap.json', 'w', encoding='utf-8') as f:
    json.dump(to_write, f, indent=2)

# next visual: bar chart of the temperature anomalies per year
anomalies_per_year = []
anomalies_years = []

for year, line in enumerate(parsed):
    year_label = line[0]
    anomalies_years.append(year_label)
    year_value = line[-1:][0]

    anomaly = None
    if year_value is not None:
        anomaly = year_value - averages_last_century[-1]

    anomalies_per_year.append(anomaly)

to_write = {
    'timestamp': datetime.datetime.now().isoformat(),
    'data': anomalies_per_year,
    'years': anomalies_years
}

with open('data/temperature-anomalies.json', 'w', encoding="utf-8") as f:
    json.dump(to_write, f, indent=2)
