import pandas as pd


def drop_columns(df, cols):
	"""
	Drop columns in a dataframe
	:param df: DataFrame: The pandas dataframe of which the columns must be dropped
	:param cols: List: The columns in the dataframe to be dropped
	:return: DataFrame: The dataframe without the given columns
	"""
	return df.drop(columns=cols)


def open_data_frame(p):
	"""
	TODO: add file type logic
	Get a dataframe based on a given path
	:param p: String: The path to the file. File can be of type that pandas can open
	:return: DataFrame: The opened dataframe
	"""
	try:
		return pd.read_csv(p)
	except FileNotFoundError:
		print(f'File "{p}" was not found')
