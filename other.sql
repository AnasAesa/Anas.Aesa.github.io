use NORTHWND
go

-- rev answer
--Sum(unitPrice * Quantity * (1-Discount)) as Rev


select RegionDescription, Sum(unitPrice * Quantity * (1-Discount)) as Rev
from [Order Details] OD join (Select OrderID, RegionDescription
                              from (select OrderID,RegionID
                              from Orders O join (select	distinct EmployeeID, RegionID
                              from EmployeeTerritories ET join Territories T ON ET.TerritoryID = T.TerritoryID)
					          N on O.EmployeeID = N.EmployeeID ) N1 join Region R on N1.RegionID = R.RegionID) N2 
							  ON OD.OrderID = N2.OrderID
							  GROUP BY RegionDescription

-- cte question

with cte1 as (
select OrderID, sum(unitPrice * Quantity * (1-Discount)) as Rev
from [Order Details]
group by OrderID),

cte2 as (select *, case when rev>400 then 'Big Order'
                        when rev<=400 and rev>=100 then 'Medium Order'
						else 'Small Order' End as OrderType
          from cte1)




select count(*) AMT, sum(Rev) Rev, OrderType from cte2 
group by OrderType




