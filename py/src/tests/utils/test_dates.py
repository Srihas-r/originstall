from datetime import date
import unittest

from datetime import datetime, timedelta

from dslib.utils.dates import DateRange, DateRangeBase


test_start_date = "2021-05-24"
test_end_date = "2021-05-30"
days = 6

# 1 week period
dates1 = DateRange(start_date=test_start_date, end_date=test_end_date)
dates2 = DateRange(end_date=test_end_date, days_from_end=days)

# single day
dates3 = DateRange(end_date=test_end_date, days_from_end=0)
dates4 = DateRange(start_date=test_end_date, end_date=test_end_date)


class TestDateRangeBase(unittest.TestCase):
    def test__format_date(self):
        obj = DateRangeBase()
        dt = datetime.now()

        expected_message = "Expected <class 'str'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            obj._format_date(dt, 123)

        expected_message = "Input parameter cannot be empty."
        with self.assertRaisesRegex(ValueError, expected_message):
            obj._format_date(123, "")

        expected_message = "Expected <class 'datetime.datetime'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            obj._format_date(123, "format")

        format = "%Y-%m-%d"
        expected = datetime.now().strftime(format)
        out = obj._format_date(dt, format)
        self.assertEqual(expected, out)

    def test__today(self):
        obj = DateRangeBase()
        expected = datetime.now().replace(microsecond=0)
        out = obj._today()
        self.assertIsInstance(out, datetime)
        self.assertEqual(expected, out.replace(microsecond=0))

    def test__yesterday(self):
        obj = DateRangeBase()
        expected = (datetime.now() - timedelta(days=1)).replace(microsecond=0)
        out = obj._yesterday()
        self.assertIsInstance(out, datetime)
        self.assertEqual(expected, out.replace(microsecond=0))

    def test__str2dt(self):
        obj = DateRangeBase()

        expected_message = "Expected <class 'str'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            obj._str2dt("dt", 123)

        expected_message = "Expected <class 'str'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            obj._str2dt(123, "dt")

        expected_message = "Input parameter cannot be empty."
        with self.assertRaisesRegex(ValueError, expected_message):
            obj._str2dt("dt", "")

        expected_message = "Input parameter cannot be empty."
        with self.assertRaisesRegex(ValueError, expected_message):
            obj._str2dt("", "format")

    def test__get_nth_dt(self):
        obj = DateRangeBase()
        dt = datetime.now()

        expected_message = "Expected <class 'int'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            obj._get_nth_dt(dt, "")

        expected_message = "Expected <class 'datetime.datetime'> object"
        with self.assertRaisesRegex(TypeError, expected_message):
            obj._get_nth_dt(123, "dt")

        expected = (datetime.now() - timedelta(days=1)).replace(microsecond=0)
        out = obj._get_nth_dt(dt, 1)
        self.assertIsInstance(out, datetime)
        self.assertEqual(expected, out.replace(microsecond=0))


class TestDateRange(unittest.TestCase):
    def test_today(self):
        dates = DateRange(end_date="today", days_from_end=days)

        # start date
        expected = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        out = dates.start_date
        self.assertEqual(expected, out)

        # end date
        expected = datetime.now().strftime("%Y-%m-%d")
        out = dates.end_date
        self.assertEqual(expected, out)

        # range
        expected = []
        for i in range(days + 1):
            expected.append((datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d"))
        expected = expected[::-1]
        out = dates.range()
        self.assertEqual(expected, out)

    def test_yesterday(self):
        dates = DateRange(end_date="yesterday", days_from_end=days)

        # start_date
        expected = (datetime.now() - timedelta(days=days + 1)).strftime("%Y-%m-%d")
        out = dates.start_date
        self.assertEqual(expected, out)

        # end_date
        expected = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        out = dates.end_date
        self.assertEqual(expected, out)

        # range
        expected = []
        for i in range(days + 1):
            expected.append(
                (datetime.now() - timedelta(days=i + 1)).strftime("%Y-%m-%d")
            )
        expected = expected[::-1]
        out = dates.range()
        self.assertEqual(expected, out)

    def test_start_date(self):
        expected = test_start_date
        objs = [dates1, dates2]
        for obj in objs:
            out = obj.start_date
            self.assertEqual(expected, out)

        expected = test_end_date
        objs = [dates3, dates4]
        for obj in objs:
            out = obj.start_date
            self.assertEqual(expected, out)

    def test_end_date(self):
        expected = test_end_date

        date_objs = [dates1, dates2, dates3, dates4]
        for date_obj in date_objs:
            out = date_obj.end_date
            self.assertEqual(expected, out)

    def test_range(self):
        # start and end dates given
        dt_list = [
            "2021-05-24",
            "2021-05-25",
            "2021-05-26",
            "2021-05-27",
            "2021-05-28",
            "2021-05-29",
            "2021-05-30",
        ]
        expected = dt_list
        out = dates1.range()
        self.assertEqual(expected, out)

        # end date and days from end given
        expected = dt_list
        out = dates2.range()
        self.assertEqual(expected, out)

    def test_bad_input(self):
        # no inputs given
        expected_message = "missing 1 required positional argument"
        with self.assertRaisesRegex(TypeError, expected_message):
            DateRange()

        # end date not given
        expected_message = "missing 1 required positional argument"
        with self.assertRaisesRegex(TypeError, expected_message):
            DateRange(start_date=test_start_date, days_from_end=6)
            # DateRange(start_date=test_start_date, days_from_end=-6)

        # only end date given
        expected_message = "You have to supply one of `days_from_end` or `start_date`"
        with self.assertRaisesRegex(Exception, expected_message):
            DateRange(end_date=test_end_date)

        # negative days from end given
        expected_message = "The `days_from_end` parameter can't be negative."
        with self.assertRaisesRegex(Exception, expected_message):
            DateRange(end_date=test_end_date, days_from_end=-6)

        expected_message = "The `days_from_end` parameter should be an integer."
        with self.assertRaisesRegex(TypeError, expected_message):
            DateRange(end_date="today", days_from_end="yesterday")

        expected_message = "`start_date` can't be greater than `end_date`"
        with self.assertRaisesRegex(Exception, expected_message):
            DateRange(end_date="yesterday", start_date="today")


if __name__ == "__main__":
    unittest.main()
