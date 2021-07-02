import time
import datetime
import util.math as pmath
import calendar

"""
This module provides various functions to manipulate date and time values.

There are two standard representations of time.  One is the number
of seconds since the Epoch, in UTC (a.k.a. GMT).  It may be an integer
or a floating point number (to represent fractions of seconds).
The Epoch is system-defined; on Unix, it is generally January 1st, 1970.
Is often represented in milliseconds or seconds.

The other representation is a tuple of 9 integers giving local time.
The tuple items are:
  year (including century, e.g. 1998)
  month (1-12)
  day (1-31)
  hours (0-23)
  minutes (0-59)
  seconds (0-59)
  weekday (0-6, Monday is 0)
  Julian day (day in the year, 1-366)
  DST (Daylight Savings Time) flag (-1, 0 or 1)
If the DST flag is 0, the time is given in the regular time zone;
if it is 1, the time is given in the DST time zone;
if it is -1, mktime() should guess based on the date and time.

Example: (2021, 4, 15, 12, 20, 20, 3, 100, 1)
"""

"""
Everything epoch timestamp
"""


def get_current_epoch_ms():
	"""
	Get the current Unix epoch (Unix time, POSIX time, Unix timestamp, Epoch timestamp) in milliseconds
	:return: Integer: Current unix epoch in milliseconds
	"""
	return int(time.time() * 1000)


def epoch_to_date_time(ts):
	"""
	TODO: Check what type gets returned and add to description [return]
	Convert the Unix epoch (Unix time, POSIX time, Unix timestamp, Epoch timestamp) to human readable datetime
	using fromtimestamp() from datetime.datetime
	:param ts: The timestamp to be converted
	:return: The converted datetime
	"""
	if pmath.get_integer_places(ts) == 10:
		return datetime.datetime.fromtimestamp(ts)
	elif pmath.get_integer_places(ts) == 13:
		return datetime.datetime.fromtimestamp(ts / 1000)


def last_hour_epoch(ts):
	"""
	Get the timestamp an hour before the given paramater
	:param ts: Integer: The timestamp to get the previous hour from.
	:return: Integer: The timestamp an hour before the given parameter
	"""
	if pmath.get_integer_places(ts) == 10:
		return ts - 3600
	elif pmath.get_integer_places(ts) == 13:
		return ts - 3600000


def get_current_date_time(f):
	"""
	Get the current datetime in a preferred format
	:param f: The format in which the current datetime must be returned
	:return: The formatted current datetime
	"""
	return datetime.datetime.now().strftime(f)


def get_month_name_by_number(n):
	"""
	Get the name of a month by number
	:param n: Integer: The month number (1-12)
	:return: String: The name of the month
	"""
	return calendar.month_name[n]
