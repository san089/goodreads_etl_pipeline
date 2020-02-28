from pyspark.sql.types import StringType,TimestampType
from pyspark.sql.functions import udf
from time import strptime
from datetime import datetime

# A Simple udf to remove extra spaces from text
remove_extra_spaces = udf(lambda x: ' '.join(x.split()) , StringType())

# A udf to transform timestamp format
@udf(TimestampType())
def stringtodatetime(datestring):
    x = datestring.split()
    day, month, year = int(x[2]), strptime(x[1],'%b').tm_mon, int(x[5])
    hour, minute, second = [int(val) for val in x[3].split(":")]
    return datetime(year = year, month = month, day = day, hour = hour, minute = minute, second = second)