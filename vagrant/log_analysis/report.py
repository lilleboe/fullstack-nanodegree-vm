
#!/usr/bin/env python2.7

import psycopg2, bleach


def main():
  '''Main method that runs various log analysis reports.'''
  
  return 0


def run_query(dbName, query):
  """Runs the given query and returns the results."""

  if (not dbName or not query):
      return None

  db = psycopg2.connect(database=dbName)
  c = db.cursor()
  c.execute(query)
  results = c.fetchall()
  db.close()
  return results


if __name__ == '__main__':
    main()