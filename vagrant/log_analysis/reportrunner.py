
#!/usr/bin/env python2.7

import psycopg2

class ReportRunner():
    '''ReportRunner class that creates various reports'''
    

    def __init__(self):
        self.dbName = 'news'

    def main(self):
      '''Main method that runs various log analysis reports.'''

      query1 = '''
        select a.title, count(a.title)
        from articles as a 
            left join log as l on position(a.slug in l.path) > 0
        group by a.title
        order by count(a.title) desc
        limit 3
      '''      
      
      query2 = '''
        select au.name, count(au.name)
        from articles as ar 
            join authors as au on ar.author = au.id
            left join log as l on position(ar.slug in l.path) > 0
        group by au.name
        order by count(au.name) desc
      '''      

      query3 = '''
        select day_total.Date, to_char((error_total.Total / day_total.Total) * 100, 'FM999.0') as Error_Rate
        from
        (select to_char(time, 'Mon DD, YYYY') as Date, cast(count(1) as decimal) as Total
         from log
         where status like '4%' or status like '5%'
         group by Date) as error_total 
        inner join
        (select to_char(time, 'Mon DD, YYYY') as Date, cast(count(1) as decimal) as Total
         from log
         group by Date) as day_total
        on error_total.Date = day_total.Date
        where (error_total.Total / day_total.Total) > 0.01
      '''      

      results = self.run_query(query1)
      self.pretty_print('What are the most popular three articles of all time?', results, ' views')

      results = self.run_query(query2)
      self.pretty_print('Who are the most popular article authors of all time?', results, ' views')

      results = self.run_query(query3)
      self.pretty_print('On which days did more than 1% of requests lead to errors?', results, '% errors')
      

    def run_query(self, query):
      """Runs the given query and returns the results."""

      if not query:
          return None

      db = psycopg2.connect(database=self.dbName)
      c = db.cursor()
      #print(query)
      c.execute(query)
      results = c.fetchall()
      db.close()
      return results

    def pretty_print(self, title, data, end):
        '''Takes data input and produces a readable output'''

        print ""
        print "---------------------------------------------------------------"
        print title
        print "---------------------------------------------------------------"
        for row in data:
            print " -- ".join([str(x) for x in row]) + end
 

if __name__ == '__main__':

    rr = ReportRunner()
    rr.main()