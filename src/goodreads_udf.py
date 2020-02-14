from pyspark.sql.types import StringType,TimestampType
from pyspark.sql.functions import udf
from time import strptime
from datetime import datetime

remove_extra_spaces = udf(lambda x: ' '.join(x.split()) , StringType())

@udf(TimestampType())
def stringtodatetime(datestring):
    x = datestring.split()
    day, month, year = int(x[2]), strptime(x[1],'%b').tm_mon, int(x[5])
    hour, minute, second = [int(val) for val in x[3].split(":")]
    return datetime(year = year, month = month, day = day, hour = hour, minute = minute, second = second)