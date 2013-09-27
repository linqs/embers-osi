#/usr/bin/python
import re

p = re.compile('POPULATION\(A, ([0-9]+)\)')

pop_lookup = {'01':"'General Population'", '02':"'Business'", '03': "'Ethnic'",'04': "'Legal'", '05': "'Education'", '06': "'Religious'", '07': "'Medical'", '08': "'Media'", '09': "'Labor'", '10': "'Refugees/Displaced'", '11': "'Agricultural'" }

for line in open('model.psl', 'r'):
	line = line.strip()
	m = p.search(line)
	if m is None:
		print line
	else:
		while m is not None:
			popNum = m.group(1)
			pop = pop_lookup[popNum];

			line = p.sub("POPULATION(A, %s)" % pop, line, 1)

			m = p.search(line)
		print line