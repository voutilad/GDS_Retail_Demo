CALL apoc.schema.assert({}, {
    Item: ["stockCode"],
    Customer: ["customerID"],
    Transaction: ["transactionID"],
    Category: ["category"],
    Country: ["name"]
}) YIELD label;

MATCH (n) DETACH DELETE n;

LOAD CSV WITH HEADERS FROM "file:///UniqueCategories.csv" AS row 
WITH row.ITEMCATEGORY as ItemCategory
MERGE (c:Category {category:ItemCategory})
RETURN COUNT (c);

LOAD CSV WITH HEADERS FROM "file:///UniqueItems.csv" AS row
WITH toInteger(row.StockCode) as StockCode,
  row.Description as Description where StockCode is not null
MERGE (i:Item {stockCode: StockCode, description:Description})
RETURN COUNT (i);

LOAD CSV WITH HEADERS FROM "file:///UniqueCountries.csv" AS row
WITH row.Country as CountryName
MERGE (c:Country {country:CountryName})
RETURN COUNT (c);

LOAD CSV WITH HEADERS FROM "file:///UniqueHouseholds.csv" AS row
WITH toInteger(row.CustomerID) as CustomerID
MERGE (c:Customer {customerID:CustomerID})
RETURN COUNT (c);

LOAD CSV WITH HEADERS FROM "file:///UniqueTransactions.csv" AS row
WITH toInteger(row.Transaction_ID) as TransactionID,
  row.InvoiceDate as InvoiceDate,
  toInteger(row.epochtime) as EpochTime
MERGE (t:Transaction {transactionID:TransactionID, invoiceDate:InvoiceDate, epochTime:EpochTime})
RETURN COUNT (t);

//Add relationships
USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///item-category.csv" as row
WITH toInteger (row.StockCode) as StockCode, row.CATEGORY as Category
MATCH (i:Item {stockCode:StockCode})
MATCH (c:Category {category:Category})
MERGE (i)-[:TYPE]->(c);

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///household-transaction.csv" as row
WITH toInteger(row.CustomerID) as CustomerID,
  toInteger(row.Transaction_ID) as TransactionID
MATCH (c:Customer {customerID:CustomerID})
MATCH (t:Transaction {transactionID:TransactionID})
MERGE (c)-[:MADE_TRANSACTION]->(t);

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///household-country.csv" as row
WITH toInteger(row.CustomerID) as CustomerID, row.Country as Country
MATCH (c:Customer {customerID:CustomerID})
MATCH (c2:Country {country:Country})
MERGE (c)-[:FROM]->(c2);

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///customer-item.csv" as row
WITH toInteger(row.NumberPurchased) as NumberPurchase,
  toInteger(row.CustomerID) as CustomerID,
  tointeger (row.StockCode) as StockCode
MATCH (c:Customer {customerID:CustomerID})
MATCH (i:Item {stockCode:StockCode})
MERGE (c)-[:BOUGHT {quantity:NumberPurchase}]->(i);

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///transaction-item.csv" as row
WITH tointeger (row.StockCode) as StockCode,
  toFloat(row.Price) as Price,
  toInteger(row.Transaction_ID) as TransactionID,
  toInteger(row.Quantity) as Quantity
MATCH (i:Item {stockCode:StockCode})
MATCH (t:Transaction {transactionID:TransactionID})
MERGE (t)-[:CONTAINS {quantity:Quantity, price:Price}]->(i);
