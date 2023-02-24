import json

def read_json():
    with open('last_sync.json') as f:
        return json.load(f)


def write_json(year, month, day):
    data = {
        "ls_year": year,
        "ls_month": month,
        "ls_day": day
    }
    with open('last_sync.json', 'w') as f:
        json.dump(data, f)