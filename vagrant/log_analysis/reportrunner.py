
#!/usr/bin/env python2.7

import psycopg2

class ReportRunner():
    '''ReportRunner class that creates various reports'''
    

    def __init__(self):
        self.dbName = 'news'

    def main(self):
      '''Main method that runs various log analysis reports.'''
  
      results = self.run_query('select * from authors')
      self.pretty_print(results)
      return 0

    def run_query(self, query):
      """Runs the given query and returns the results."""

      if not query:
          return None

      db = psycopg2.connect(database=self.dbName)
      c = db.cursor()
      c.execute(query)
      results = c.fetchall()
      db.close()
      return results

    def pretty_print(self, data):
        '''Takes data input and produces a readable output'''
        for row in data:
            print(row)
 

if __name__ == '__main__':
    rr = ReportRunner()
    rr.main()