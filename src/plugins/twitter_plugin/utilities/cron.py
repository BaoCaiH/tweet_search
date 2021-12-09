from ast import parse
import re
from typing import Mapping

map_day_of_week = {
    "sunday": 0,
    "sun": 0,
    "monday": 1,
    "mon": 1,
    "tuesday": 2,
    "tue": 2,
    "wednesday": 3,
    "wed": 3,
    "thursday": 4,
    "thu": 4,
    "friday": 5,
    "fri": 5,
    "saturday": 6,
    "sat": 6,
}
map_month = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}
map_ = dict()
map_.update(map_day_of_week)
map_.update(map_month)

def parse_element(element, min=0, max=59):
    # * any value
    # , value list
    # range of value
    # step value
    # 0-59
    if element is None:
        return "*"
    if isinstance(element, str):
        pattern_every = re.compile("^every [a-zA-Z]*$")
        pattern_every_unit = re.compile("^every [0-9]+ .*$")
        pattern_from_to = re.compile("^from [0-9]+ to [0-9]+$")
        pattern_from_dash_to = re.compile("^[0-9]+-[0-9]+$")
        pattern_from_to_text = re.compile("^from [a-z]+ to [a-z]+$")
        pattern_from_dash_to_text = re.compile("^[a-z]+-[a-z]+$")
        pattern_star = re.compile("^\*$")
        pattern_comma_delimited = re.compile("^([0-9]+,)*[0-9]+$")
        pattern_comma_delimited_text = re.compile("^([a-z]+,)*[a-z]+$")
        pattern_step = re.compile("^\*?/[0-9]+$")
        if pattern_every.match(element):
            keywords = element.split(" ")
            if keywords[1] in list(map_.keys()):
                assert min <= map_.get(keywords[1]) <= max
                return f"{map_.get(keywords[1])}"
            return "*"
        elif pattern_every_unit.match(element):
            keywords = element.split(" ")
            assert min <= int(keywords[1]) <= max
            return f"*/{keywords[1]}"
        elif pattern_from_to.match(element):
            keywords = element.split(" ")
            from_ = int(keywords[1])
            to_ = int(keywords[3])
            assert min <= from_ <= max
            assert min <= to_ <= max
            assert from_ < to_
            return f"{from_}-{to_}"
        elif pattern_from_dash_to.match(element):
            keywords = element.split("-")
            from_ = int(keywords[0])
            to_ = int(keywords[1])
            assert min <= from_ <= max
            assert min <= to_ <= max
            assert from_ < to_
            return f"{from_}-{to_}"
        elif pattern_from_to_text.match(element):
            keywords = element.split(" ")
            from_ = map_.get(keywords[1])
            to_ = map_.get(keywords[3])
            assert min <= from_ <= max
            assert min <= to_ <= max
            assert from_ < to_
            return f"{from_}-{to_}"
        elif pattern_from_dash_to_text.match(element):
            keywords = element.split("-")
            from_ = map_.get(keywords[0])
            to_ = map_.get(keywords[1])
            assert min <= from_ <= max
            assert min <= to_ <= max
            assert from_ < to_
            return f"{from_}-{to_}"
        elif pattern_star.match(element):
            return element
        elif pattern_comma_delimited.match(element):
            keywords = element.split(",")
            assert all([min <= int(n) <= max for n in keywords])
            return element
        elif pattern_comma_delimited_text.match(element):
            keywords = element.split(",")
            assert all([min <= map_.get(n) <= max for n in keywords])
            return ",".join([str(map_.get(n)) for n in keywords])
        elif pattern_step.match(element):
            keywords = element.split("/")
            assert min <= int(keywords[-1]) <= max
            return f"*/{keywords[-1]}"
        else:
            raise Exception("""
                The input string does not match any of the following patterns:
                    - '*'
                    - 'every [unit]'
                    - 'every [number] [unit (minute, month, etc.)]'
                    - 'from [number] to [number]'
                    - '[number]-[number]'
                    - '[number],[number],[number],..'
                    - '*/[number]'
                    only applicable to element with names
                    - 'every [text unit (tue or apr)]'
                    - 'from [text] to [text]'
                    - '[text]-[text]'
                    - '[text],[text],[text],..'
            """)
    elif isinstance(element, int):
        assert min <= element <= max
        return f"{element}"
    elif isinstance(element, list):
        assert all([isinstance(n, int) for n in element])
        return ",".join([str(n) for n in element])
    else:
        raise Exception(f"Not supporting this input format: {element}.")

def parse_cron(
    minute=None,
    hour=None,
    day_of_month=None,
    month=None,
    day_of_week=None
):
    return " ".join([
        parse_element(minute),
        parse_element(hour, 0, 23),
        parse_element(day_of_month, 1, 31),
        parse_element(month, 1, 12),
        parse_element(day_of_week, 0, 6)
    ])

if __name__ == "__main__":
    assert parse_cron() == "* * * * *"
    assert parse_cron(minute="every 5 minute") == "*/5 * * * *"
    assert parse_cron(minute=0, hour="20") == "0 20 * * *"
    assert parse_cron(minute=0, hour=1, day_of_week="every tuesday") == "0 1 * * 2"
    assert parse_cron(minute=0, hour=1, day_of_week="tuesday,wed,fri") == "0 1 * * 2,3,5"
    assert parse_cron(minute=0, hour=1, day_of_week="tuesday-fri") == "0 1 * * 2-5"
    assert parse_cron(minute=0, hour=1, month="every 2 month", day_of_week="tuesday-fri") == "0 1 * */2 2-5"
    
    print(parse_cron(minute=0, hour=1, month="every 2 month", day_of_week="tuesday-fri"))