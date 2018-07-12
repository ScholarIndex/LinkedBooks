# -*- coding: utf-8 -*-
"""
Loads checked and correct disambiguations AND a sample of references which are NOT checked-
"""
__author__ = """Giovanni Colavizza"""

import string,re

def cleanup(text):
	"""
	remove punctuation except . and numbers
	:param text: a string
	:return: a string
	"""

	RE_D = re.compile('\d')

	tokens = text.split()
	new_tokens = list()
	for t in tokens:
		if RE_D.search(t):
			continue
		for p in string.punctuation:
			if p == ".":
				continue
			t=t.replace(p,"")
		new_tokens.append(t.lower().strip())

	return " ".join(new_tokens)
