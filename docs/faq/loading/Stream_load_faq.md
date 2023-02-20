# Stream Load

## Does Stream Load identify the column name of the first row in a text file? Or can I specify the first row not to be read？

Stream Load cannot identify the column name of the first row as it is not differentiated from other data. Currently nor can it be specified to read the first rows. If users need to specify the first row of the file as the column name, they can do it in the following ways:

1. Modify settings in the export tool and export once again the text-based data files without column names.
2. Use commands such as sed -i '1d' filename to delete the first rows in text files.
3. Use -H "where: 列名 != '列名称'" in Stream Load statement to filter this column. When the first row strings cannot be converted to other types, they will be returned as NULL. Thus, this requires the table fields not to be set as NOT NULL.
4. Add -H "max_filter_ratio:0.01" into Stream Load statement and based on the data volume give it a fault tolerance rate of 1% or below that could tolerate errors in the first row. After adding the tolerance rate, ErrorURL that returns results will still report errors but without affecting the success of overall tasks. The tolerance rate should not be set too high as it may miss other data issues.

## The data corresponding to the current partition key is not the standard date or int. It is in the format of 202106.00 for example. If Stream Load is needed to perform data import to StarRocks, how will the data be converted?

StarRocks supports data conversion during imports. You can refer to "4.7 data conversion during imports" in the enterprise edition.

Let's take Stream Load as an example, suppose the table TEST has four columns NO, DATE, VERSION and PRICE, and the exported CSV data file has the DATE field in the unstandardised 202106.00 format. If the partition column to be used in StarRocks is DATE, then first we need to create a table in StarRocks, specifying the DATE type as date, datetime or int. After that, in the Stream Load command, use this to achieve data conversion in the four columns:

```plain text
-H "columns: NO,DATE_1, VERSION, PRICE, DATE=LEFT(DATE_1,6)"
```

DATE_1 can simply be thought of as a placeholder for the number to be taken and then converted by the function to the corresponding field in StarRocks. Note in particular that we need to list all the columns in the CSV data file before we perform the function conversion. Common functions are all available here.
