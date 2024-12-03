from pathlib import Path
import datetime as dt
import polars as pl

from icalendar import Calendar, Event


def load_contact_birthdays(contacts_path: Path) -> pl.DataFrame:
    return (
        pl.read_csv(contacts_path)
        .select(
            pl.concat_str(
                pl.col("First Name"),
                pl.col("Middle Name"),
                pl.col("Last Name"),
                separator=" ",
                ignore_nulls=True,
            ).alias("name"),
            (
                pl.when(pl.col("Birthday").str.starts_with(" -"))
                .then(
                    pl.lit(str(dt.datetime.now().year))
                    + pl.col("Birthday").str.slice(offset=2)
                )
                .otherwise(pl.col("Birthday"))
            )
            .str.to_date()
            .alias("birthday"),
        )
        .drop_nulls()
    )


def main():
    import argparse

    parser = argparse.ArgumentParser(
        prog="birthday-calendar",
        description="Converts Google Contacts export to a birthday calendar.",
    )
    parser.add_argument(
        "contacts",
        type=Path,
        help="Path to the csv export of the Google contacts.",
    )
    parser.add_argument(
        "calendar",
        type=Path,
        help="Path to store the created ics calendar file to.",
    )
    args = parser.parse_args()

    birthdays = load_contact_birthdays(args.contacts)

    cal = Calendar()
    for name, birthday in birthdays.iter_rows():
        event = Event()
        event.add("summary", f"{name}")
        event.add("dtstart", birthday)
        event.add("rrule", {"freq": "yearly"})
        cal.add_component(event)

    with open(args.calendar, mode="wb") as ics_file:
        ics_file.write(cal.to_ical())


if __name__ == "__main__":
    main()
