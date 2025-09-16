# WindowFunctionCode
<h2>Description</h2>
You work at an e commerce company. The orders dataset has multiple purchase per customer. The business team wants to know:
Each Customer's latest order and Each customers total spend
<br />
<br/>
<p align="center">
Input Raw Data:  <br/>
<img src="https://github.com/user-attachments/assets/89e45be9-8820-4bb1-8436-8205874d8515" height="40%" width="40%" alt="Disk Sanitization Steps"/>
</p>
<br />
<br/>
<p align="center">
Output Process Data:  <br/>
<img src="https://github.com/user-attachments/assets/168f179d-62b2-41fc-af71-e06367f9878e" height="40%" width="40%" alt="Disk Sanitization Steps"/>
</p>
<h2>Things need to do</h2>
- <b>Window it first</b> 
- <b>Find the total</b>

<h2>Import Window and functions from pySpark </h2>

- <b>Raw data is given convert it into DF</b> (21H2)

<h2>Raw Code:</h2>
data = [
</p>
    (1, "2025-09-01", 100),
</p>
    (1, "2025-09-05", 200),
</p>
    (2, "2025-09-03", 150),
</p>
    (2, "2025-09-07", 300),
</p>
    (3, "2025-09-02", 400)
</p>
]
</p>
columns = ["customer_id","order_date","amount"]
</p>
df= spark.createDataFrame(data, schema = columns)
</p>
df.show()

</p>
from pyspark.sql.window import Window
</p>
from pyspark.sql.functions import *

</p>
createwindow = Window.partitionBy("customer_id").orderBy(desc("order_date"))
</p>
denserankdf = df.withColumn("denserank", dense_rank().over(createwindow))
</p>
denserankdf.show()

</p>
fildf = denserankdf.filter("denserank = 1 ").drop("denserank")
</p>
fildf.show()

</p>
dftotal = df.groupBy("customer_id").agg(
</p>
    sum("amount").alias("total_spend")).orderBy(asc("customer_id"))
</p>
dftotal.show()

</p>
finaldf = fildf.join(dftotal,["customer_id"], "full")
</p>
finaldf.show()

<p align="center">

<h2>Code walk-through:</h2>
First I change the raw data to Dataframe then created window so that I can take the first customer order it and do the customer wise split and then filter it after filter just perform full join.
</p>

<!--
 ```diff
- text in red
+ text in green
! text in orange
# text in gray
@@ text in purple (and bold)@@
```
--!>
