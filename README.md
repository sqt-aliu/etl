# etl
Extract Transform Load - Asia Pacific Historical Data

# Initial Database Load

1. Load FactSet (approx 4 hrs) Prices - 38,395,130 rows / Divs - 150680 rows
/home/sqtdata/etl/load/bin/record-factset-quotes.py -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-factset-divs.py -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
2. Load Japan (approx 2 hrs)
/home/sqtdata/etl/load/bin/record-jpx-reports.py -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
3. Load Australia (approx 2 hrs)
/home/sqtdata/etl/load/bin/record-asx-prices.py -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
4. Load Yahoo (GapFill) - Missing data between Oct '16 - May '17
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t hkex -i /dfs/stage/yahoo.gapfill -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t szse -i /dfs/stage/yahoo.gapfill -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t sse -i /dfs/stage/yahoo.gapfill -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
5. Load Yahoo 
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t hkex -d 'mysgit sql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t szse -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t sse -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-yahoo-quotes.py -t asx -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
6. Load AAStock Dividends
/home/sqtdata/etl/load/bin/record-aastocks-dividends.py -t hkex -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-aastocks-dividends.py -t szse -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
/home/sqtdata/etl/load/bin/record-aastocks-dividends.py -t sse -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'
7. Load Overrides
/home/sqtdata/etl/load/bin/record-override-quotes.py -d 'mysql+mysqlconnector://sqtdata:sqtdata123@10.59.150.61:3306/equities'