import math


def get_integer_places(n):
	"""
	Calculates the amount of places of an integer using log10() of the math module.
	:param n: Integer: The number of which the amount of digits will be calculated.
	:return: Integer: The amount of digits the parameter number contains.
	Requires an integer.

	Another, but slower option is to use: len(str())

	IMPORTANT: Gives problems when the number is greater than 999999999999997 (15 places).
	The float then has too many 9sm causing the result to round up.
	Therefore a check is executed to check whether the absolute value of the parameter is not greater than that,
	consequently not adding up a '1'.
	"""
	if n != 0:
		if abs(n) <= 999999999999997:
			return int(math.log10(abs(n))) + 1
		else:
			return int(math.log10(abs(n)))
	else:
		return 1
