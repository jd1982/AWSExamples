orders = LOAD '/user/ubuntu/input/orders' USING PigStorage(',') AS (customerId,itemId,quantity,pricePerItem);
groupedorders = GROUP orders by itemId;
itemcounts = foreach groupedorders generate group as itemId, SUM(orders.quantity);
STORE itemcounts INTO '/user/ubuntu/output/groupedorders' USING PigStorage();